"""
Module 02b: Case Log Transformer
--------------------------------
Second half of the transformation stage: builds the case table
(process_mining_cases) from the event log produced by 02a_transform_event_log.
Runs unconditionally and never needs per-workflow instrumentation to succeed.

Two layers, merged on case_id:
  Layer 1 - generic metrics computed for every case from the event log alone:
            start, end, throughput, total tokens, activity count, error count, PII flag.
  Layer 2 - business attributes read from the root execution's customData (optional
            values a workflow author writes via an n8n "Execution Data" node). Each
            customData key becomes its own column; if a workflow was never
            instrumented, those columns are simply NULL.
"""

import os
import sys
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CASE_BUILDER")

load_dotenv()

RAW_TABLE = "raw_api_executions"
EVENTS_TABLE = "process_mining_events"
CASES_TABLE = "process_mining_cases"

# Generic columns are always present; a customData key colliding with one of these issuffixed so the generic metric is never overwritten.
GENERIC_COLUMNS = {"case_id", "root_workflow_id", "workflow_name",
                   "start_time", "end_time", "throughput_sec",
                   "total_tokens", "activity_count", "error_count", "pii_in_case"}

def get_target_engine():
    """Create the SQLAlchemy engine for the target PostgreSQL warehouse."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    db = os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def _coerce_custom_data(value) -> dict:
    """Normalize a customData value (JSONB dict, JSON string, or None) into a dict."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def build_generic_layer(engine) -> pd.DataFrame:
    """Layer 1: one row per case with the always-available metrics and identity."""
    events = pd.read_sql(
        f"SELECT case_id, root_workflow_id, start_timestamp, end_timestamp, "
        f"token_usage, execution_status, pii_detected FROM {EVENTS_TABLE}",
        engine,
    )
    if events.empty:
        logger.error(f"{EVENTS_TABLE} is empty - run the transformer (02a) first")
        sys.exit(1)

    events["start_timestamp"] = pd.to_datetime(events["start_timestamp"])
    events["end_timestamp"] = pd.to_datetime(events["end_timestamp"])

    grouped = events.groupby("case_id", dropna=False)
    cases = grouped.agg(
        root_workflow_id=("root_workflow_id", "first"),
        start_time=("start_timestamp", "min"),
        end_time=("end_timestamp", "max"),
        total_tokens=("token_usage", "sum"),
        activity_count=("case_id", "size"),
    ).reset_index()
    cases["throughput_sec"] = (cases["end_time"] - cases["start_time"]).dt.total_seconds().round(3)
    cases["error_count"] = grouped["execution_status"].apply(lambda s: int((s != "success").sum())).reset_index(drop=True)
    cases["pii_in_case"] = grouped["pii_detected"].any().reset_index(drop=True)
    return cases

def resolve_workflow_names(engine, workflow_ids) -> dict:
    """Map each root_workflow_id to its human-readable workflow name."""
    names = {}
    if not [w for w in workflow_ids if w is not None and w != "unresolved"]:
        return names
    try:
        raw = pd.read_sql(f'SELECT DISTINCT "workflowId" AS wid, "workflowData" AS wd FROM {RAW_TABLE}', engine)
        for _, r in raw.iterrows():
            wd = r["wd"]
            if isinstance(wd, str):
                try:
                    wd = json.loads(wd)
                except Exception:
                    wd = None
            if isinstance(wd, dict) and wd.get("name"):
                names[str(r["wid"])] = wd["name"]
    except Exception as e:
        logger.warning(f"Could not resolve workflow names: {e}")
    return names

def build_business_layer(engine, case_ids) -> pd.DataFrame:
    """Layer 2: flatten each case's root customData into columns.

    case_id is the root execution id, so it joins straight onto raw_api_executions.id.
    Missing or empty customData yields no business values (NULL columns after merge).
    """
    raw = pd.read_sql(f'SELECT id, "customData" AS custom_data FROM {RAW_TABLE}', engine)
    raw["id"] = raw["id"].astype(str)
    custom_by_id = {row["id"]: _coerce_custom_data(row["custom_data"]) for _, row in raw.iterrows()}

    rows = []
    for cid in case_ids:
        record = {"case_id": cid}
        for key, val in custom_by_id.get(str(cid), {}).items():
            col = key if key not in GENERIC_COLUMNS else f"{key}_attr"
            # Keep scalars as-is; serialize nested structures so the column stays atomic.
            record[col] = val if not isinstance(val, (dict, list)) else json.dumps(val, ensure_ascii=False)
        rows.append(record)
    return pd.DataFrame(rows)

def build_case_log():
    """Build the case table by merging the generic and business layers, then store it."""
    try:
        engine = get_target_engine()

        generic = build_generic_layer(engine)
        names = resolve_workflow_names(engine, generic["root_workflow_id"].dropna().unique().tolist())
        generic["workflow_name"] = generic["root_workflow_id"].map(lambda w: names.get(str(w)))

        business = build_business_layer(engine, generic["case_id"].tolist())
        cases = generic.merge(business, on="case_id", how="left")

        # Stable column order: identity, generic metrics, then business attributes.
        lead = ["case_id", "root_workflow_id", "workflow_name", "start_time", "end_time",
                "throughput_sec", "total_tokens", "activity_count", "error_count", "pii_in_case"]
        business_cols = sorted([c for c in cases.columns if c not in lead])
        cases = cases[lead + business_cols]

        cases.to_sql(CASES_TABLE, engine, if_exists="replace", index=False)
        logger.info(f"Built {len(cases)} cases ({len(business_cols)} business attribute column(s))")
    except Exception as e:
        logger.error(f"Failed to build case log: {e}")
        sys.exit(1)

if __name__ == "__main__":
    build_case_log()