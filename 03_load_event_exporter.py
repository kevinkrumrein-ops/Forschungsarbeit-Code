"""
Module 03: Event Log Exporter
-----------------------------
Fetches processed event data from PostgreSQL and exports it to a standardized CSV file.
Prepares the final dataset for seamless, real-time ingestion into Celonis EMS.

The mandatory activity table is always exported. A case table is exported in addition
if an optional workflow-specific case builder has populated 'process_mining_cases'.

Set EXPORT_SPLIT_BY_WORKFLOW=true to materialize one activity file per *root* workflow
(recommended for Celonis, which treats one activity table as exactly one process). The
split uses 'root_workflow_id' so a case that spans a main workflow plus its tool
sub-workflows stays intact in a single file. Files are named after the workflow name.
"""

import os
import re
import sys
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

# Uniform logging configuration matching the pipeline architecture
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("EXPORTER")

load_dotenv()

def get_target_engine():
    """Initializes and returns the SQLAlchemy connection engine for PostgreSQL."""
    user = os.getenv('TARGET_DB_USER')
    pw = os.getenv('TARGET_DB_PASSWORD')
    host = os.getenv('TARGET_DB_HOST', '127.0.0.1')
    port = os.getenv('TARGET_DB_PORT', '5434')
    db = os.getenv('TARGET_DB_NAME')
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def _sanitize_filename(name: str) -> str:
    """Turns an arbitrary workflow name into a safe filename fragment."""
    safe = re.sub(r'[^0-9A-Za-z._-]+', '_', str(name)).strip('_')
    return safe or 'workflow'

def resolve_workflow_names(engine, workflow_ids) -> dict:
    """
    Looks up the human-readable workflow name for each (stable) workflow_id by reading
    one raw execution per workflow. Defensive: any id that cannot be resolved keeps its
    raw id as the fallback name, so the export never fails on a missing/odd record.
    """
    names = {}
    for wid in workflow_ids:
        fallback = _sanitize_filename(wid)
        try:
            df = pd.read_sql(
                text('SELECT "workflowData" AS wd FROM raw_api_executions WHERE "workflowId" = :wid LIMIT 1'),
                engine, params={"wid": wid}
            )
            if df.empty:
                names[wid] = fallback
                continue
            wd = df.iloc[0]['wd']
            if isinstance(wd, str):
                wd = json.loads(wd)
            name = wd.get('name') if isinstance(wd, dict) else None
            names[wid] = _sanitize_filename(name) if name else fallback
        except Exception:
            names[wid] = fallback
    return names

def export_event_log(engine, output_file: str) -> int:
    """Materializes the mandatory activity table into a Celonis-ready CSV.

    By default everything is written to a single file (with 'workflow_id' and
    'root_workflow_id' columns so workflows stay distinguishable). If
    EXPORT_SPLIT_BY_WORKFLOW is enabled, one file per root workflow is written instead,
    named after the workflow. No interactive input is required - the split is driven
    purely by the root_workflow_id already present in the data.
    """
    # Retrieve data using strict process mining sorting constraints (Case ID & Chronological order)
    query = "SELECT * FROM process_mining_events ORDER BY case_id, end_timestamp ASC"
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.error("[EXPORTER] Export failed: Table 'process_mining_events' is empty.")
        sys.exit(1)

    # Map the primary time reference required by the downstream Celonis schema
    df['timestamp'] = df['end_timestamp']

    target_columns = [
        'case_id', 'workflow_id', 'root_workflow_id', 'activity', 'previous_activity',
        'execution_index', 'timestamp', 'execution_time_sec', 'token_usage',
        'data_volume_bytes', 'pii_detected', 'execution_status', 'error_type', 'system_overhead_sec'
    ]
    existing_columns = [col for col in target_columns if col in df.columns]
    df_export = df[existing_columns]

    # Optional: one activity file per ROOT workflow so Celonis builds separate process
    # models, while keeping each case (incl. its tool sub-workflows) whole.
    split_by_workflow = os.getenv("EXPORT_SPLIT_BY_WORKFLOW", "false").strip().lower() in ("1", "true", "yes")
    if split_by_workflow and 'root_workflow_id' in df_export.columns:
        base, ext = os.path.splitext(output_file)
        ext = ext or ".csv"

        # 'unresolved' is a sentinel for sub-runs whose parent execution is missing from
        # the warehouse (referential gap). It is NOT a real workflow, so we never look it
        # up by name - it gets its own clearly flagged file so gaps surface immediately.
        real_ids = [w for w in df_export['root_workflow_id'].dropna().unique().tolist() if w != "unresolved"]
        name_map = resolve_workflow_names(engine, real_ids)

        used_names = {}
        total_rows = 0
        for wf_id, group in df_export.groupby('root_workflow_id', dropna=False):
            if pd.isna(wf_id) or wf_id == "unresolved":
                wf_name = "UNRESOLVED_referential_gap"
            else:
                wf_name = name_map.get(wf_id, _sanitize_filename(wf_id))
                # Guard against two different workflows sharing the same name
                if wf_name in used_names and used_names[wf_name] != wf_id:
                    wf_name = f"{wf_name}_{_sanitize_filename(wf_id)}"
                used_names[wf_name] = wf_id

            wf_file = f"{base}_{wf_name}{ext}"
            group.to_csv(wf_file, index=False, encoding='utf-8', sep=',')
            level = "WARNING" if wf_name.startswith("UNRESOLVED") else "INFO"
            logger.log(getattr(logging, level),
                       f"[EXPORTER] Wrote {len(group)} events for '{wf_name}' to '{wf_file}'.")
            total_rows += len(group)
        return total_rows

    # Materialize the structured dataset to disk (single file)
    df_export.to_csv(output_file, index=False, encoding='utf-8', sep=',')
    return len(df_export)

def try_export_case_log(engine, output_file: str) -> int:
    """Materializes the case table to CSV if an optional case builder has populated it.

    Returns 0 silently if the case table does not exist or is empty, so the pipeline
    remains fully functional for workflows without a case-builder script.
    """
    if not inspect(engine).has_table('process_mining_cases'):
        return 0

    df = pd.read_sql("SELECT * FROM process_mining_cases ORDER BY case_id ASC", engine)
    if df.empty:
        return 0

    logger.info(f"[EXPORTER] Exporting case log to '{output_file}'.")
    df.to_csv(output_file, index=False, encoding='utf-8', sep=',')
    return len(df)

def export_logs():
    """Extracts processed logs from the warehouse and materializes them to disk."""
    event_file = os.getenv("EXPORT_PATH",      "Event_Logs.csv")
    case_file  = os.getenv("CASE_EXPORT_PATH", "Case_Logs.csv")

    logger.info(f"[EXPORTER] Exporting event log to '{event_file}'.")

    try:
        engine = get_target_engine()
        event_count = export_event_log(engine, event_file)
        case_count  = try_export_case_log(engine, case_file)
    except Exception as e:
        logger.error(f"[EXPORTER] Critical failure during CSV export: {str(e)}")
        sys.exit(1)

    summary = f"{event_count} events" + (f" and {case_count} cases" if case_count else "")
    logger.info(f"[EXPORTER] Export finished. Materialized {summary} to disk.")

if __name__ == "__main__":
    export_logs()
