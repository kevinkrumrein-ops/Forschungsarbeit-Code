"""
Optional Module: Invoice Case Log Builder
-----------------------------------------
Workflow-specific extension of the generic process mining pipeline.
 
This script enriches the workflow-agnostic event log (Module 02) with a case
table for the invoice processing workflow. It is optional: the core ETL pipeline
produces a complete event log for any n8n workflow without it. For other
workflows, write a sibling script (e.g. optional_case_log_ticketing.py) that
follows the same pattern.
"""
 
import os
import sys
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
 
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("CASE-BUILDER")
 
load_dotenv()
 
# Node names this script depends on. Changing the invoice workflow requires updating these.
NODE_AI_CLASSIFICATION = 'Parse AI Classification'
NODE_BI_ANALYSIS       = 'Parse BI Analysis'
NODE_ARCHIVE_AUTO      = 'Move File Archiv_Automatisch'
NODE_ARCHIVE_MANUAL    = 'Move File Archiv_Manuell'
 
def get_target_engine():
    """Initializes and returns the SQLAlchemy connection engine for PostgreSQL."""
    user = os.getenv("TARGET_DB_USER")
    pw   = os.getenv("TARGET_DB_PASSWORD")
    db   = os.getenv("TARGET_DB_NAME")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")
 
def ensure_cases_table(engine) -> None:
    """Provisions the case table on first run with an explicit, stable schema."""
    ddl = """
        CREATE TABLE IF NOT EXISTS process_mining_cases (
            case_id                 TEXT PRIMARY KEY,
            invoice_number          TEXT,
            invoice_date            DATE,
            vendor                  TEXT,
            category                TEXT,
            net_amount              NUMERIC,
            tax_amount              NUMERIC,
            total_amount            NUMERIC,
            approval_path           TEXT,
            extraction_successful   BOOLEAN,
            discount_granted        BOOLEAN,
            discount_amount         NUMERIC,
            new_total               NUMERIC,
            upsell_suggested        BOOLEAN,
            anomaly_detected        BOOLEAN,
            extraction_confidence   NUMERIC,
            case_start_timestamp    TIMESTAMP,
            case_end_timestamp      TIMESTAMP,
            case_duration_sec       NUMERIC,
            total_tokens            INTEGER,
            activity_count          INTEGER,
            error_count             INTEGER,
            pii_in_case             BOOLEAN
        )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
 
def get_already_built_case_ids(engine) -> set:
    """Returns the set of case IDs already present in the case table for incremental builds."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text('SELECT case_id FROM process_mining_cases')).fetchall()
            return {r[0] for r in rows}
    except Exception:
        return set()
 
def fetch_unprocessed_executions(engine, already_built: set) -> str:
    """Builds a SQL query that returns only executions whose case has not been built yet."""
    if not already_built:
        return 'SELECT id, data FROM raw_api_executions ORDER BY CAST(id AS INTEGER) ASC'
    excluded = ",".join(f"'{cid}'" for cid in already_built)
    return f"""
        SELECT id, data FROM raw_api_executions
        WHERE CAST(id AS TEXT) NOT IN ({excluded})
        ORDER BY CAST(id AS INTEGER) ASC
    """
 
def _get_node_json(run_data: dict, node_name: str) -> dict:
    """Returns the first run's JSON payload for the given node, or an empty dict."""
    runs = run_data.get(node_name)
    if not runs:
        return {}
    try:
        return runs[0].get('data', {}).get('main', [[{}]])[0][0].get('json', {}) or {}
    except (IndexError, KeyError, TypeError, AttributeError):
        return {}
 
def _parse_payload(raw_api_data) -> dict:
    """Normalizes the raw n8n payload into a Python dict (PostgreSQL JSONB sometimes yields strings)."""
    if isinstance(raw_api_data, dict):
        return raw_api_data
    try:
        result = json.loads(raw_api_data or '{}')
        return result if isinstance(result, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}
 
def extract_case_summary(execution_id, raw_api_data) -> dict:
    """
    Builds a single case-level record from a parent workflow execution.
    Returns None for sub-workflows (whose events are already merged into the parent case).
    """
    try:
        payload = _parse_payload(raw_api_data)
 
        # Skip sub-workflows: their events already belong to the parent case
        if not payload or payload.get('parentExecution', {}).get('executionId'):
            return None
 
        run_data = payload.get('resultData', {}).get('runData', {})
        if not run_data:
            return None
 
        # Business attributes are emitted by dedicated AI nodes in the invoice workflow
        classification = _get_node_json(run_data, NODE_AI_CLASSIFICATION)
        bi_analysis    = _get_node_json(run_data, NODE_BI_ANALYSIS)
 
        # Approval path is inferred from which archive branch actually ran
        if NODE_ARCHIVE_AUTO in run_data:
            approval_path = "Automatisch"
        elif NODE_ARCHIVE_MANUAL in run_data:
            approval_path = "Manuell"
        else:
            approval_path = "Unknown"
 
        # The upsell field may arrive as a string or as a structured object depending on the LLM
        discount_amount = float(bi_analysis.get('loyaltyDiscountAmount') or 0)
        upsell_raw = bi_analysis.get('upsellSuggestion')
        upsell_present = bool(upsell_raw) and (not isinstance(upsell_raw, str) or upsell_raw.strip() != '')
 
        return {
            "case_id":                str(execution_id),
            "invoice_number":         classification.get('invoiceNumber'),
            "invoice_date":           classification.get('date'),
            "vendor":                 classification.get('vendor'),
            "category":               classification.get('category'),
            "net_amount":             classification.get('netAmount'),
            "tax_amount":             classification.get('taxAmount'),
            "total_amount":           classification.get('totalAmount'),
            "approval_path":          approval_path,
            "extraction_successful":  bool(classification),
            "discount_granted":       discount_amount > 0,
            "discount_amount":        discount_amount,
            "new_total":              bi_analysis.get('newTotal'),
            "upsell_suggested":       upsell_present,
            "anomaly_detected":       bool(classification.get('anomalyDetected')),
            "extraction_confidence":  classification.get('confidence'),
        }
    except Exception as e:
        logger.error(f"[CASE-BUILDER] Failed to build case summary for execution {execution_id}: {str(e)}")
        return None
 
def enrich_cases_with_event_metrics(engine, case_ids: list) -> pd.DataFrame:
    """Aggregates technical metrics from the event log via PostgreSQL GROUP BY.
 
    Casts case_id to TEXT on both sides to handle event tables where pandas may
    have provisioned the column as a numeric type instead of TEXT.
    """
    if not case_ids:
        return pd.DataFrame()
 
    placeholders = ",".join(f"'{cid}'" for cid in case_ids)
    query = f"""
        SELECT
            CAST(case_id AS TEXT)                                           AS case_id,
            MIN(start_timestamp)                                            AS case_start_timestamp,
            MAX(end_timestamp)                                              AS case_end_timestamp,
            EXTRACT(EPOCH FROM (MAX(end_timestamp) - MIN(start_timestamp))) AS case_duration_sec,
            SUM(token_usage)                                                AS total_tokens,
            COUNT(*)                                                        AS activity_count,
            SUM(CASE WHEN execution_status != 'success' THEN 1 ELSE 0 END)  AS error_count,
            BOOL_OR(pii_detected)                                           AS pii_in_case
        FROM process_mining_events
        WHERE CAST(case_id AS TEXT) IN ({placeholders})
        GROUP BY case_id
    """
    return pd.read_sql(query, engine)
 
def build_invoice_cases():
    """Builds case-level records for all parent workflow executions not yet processed."""
    engine = get_target_engine()
    ensure_cases_table(engine)
 
    already_built = get_already_built_case_ids(engine)
    logger.info(f"[CASE-BUILDER] Searching for new cases ({len(already_built)} cases already present).")
 
    query = fetch_unprocessed_executions(engine, already_built)
    total_new_cases = 0
 
    try:
        for chunk in pd.read_sql(query, engine, chunksize=100):
            new_cases = []
            for _, row in chunk.iterrows():
                case_record = extract_case_summary(row['id'], row.get('data', {}))
                if case_record:
                    new_cases.append(case_record)
 
            if new_cases:
                df_cases = pd.DataFrame(new_cases)
 
                # Enrich each case with aggregated runtime metrics from the event log
                metrics_df = enrich_cases_with_event_metrics(engine, df_cases['case_id'].tolist())
                if not metrics_df.empty:
                    df_cases = df_cases.merge(metrics_df, on='case_id', how='left')
 
                df_cases.to_sql('process_mining_cases', engine, if_exists='append', index=False)
                total_new_cases += len(df_cases)
    except Exception as e:
        logger.error(f"[CASE-BUILDER] Critical pipeline failure: {str(e)}")
        sys.exit(1)
 
    logger.info(f"[CASE-BUILDER] Case construction finished. Appended {total_new_cases} new cases to database.")
 
if __name__ == "__main__":
    build_invoice_cases()