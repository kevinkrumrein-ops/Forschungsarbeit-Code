"""
Module 03: Event Log Exporter
-----------------------------
Reads the processed tables from PostgreSQL and writes Celonis-ready CSV files.

The event table is always exported; the case table is exported in
addition when 02b_transform_case_log has populated it.

With EXPORT_SPLIT_BY_WORKFLOW=true, one file per root workflow is written
(recommended for Celonis, which treats one activity table as one process). The
split keys on root_workflow_id, so a case spanning a main workflow and its tool
sub-workflows stays in a single file. Files are named after the workflow.
"""

import os
import re
import sys
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("EXPORTER")

load_dotenv()

RAW_TABLE = "raw_api_executions"
EVENTS_TABLE = "process_mining_events"
CASES_TABLE = "process_mining_cases"

# Sentinel for the UNRESOLVED (referential-gap) group: sub-runs whose parent execution is missing. Not a real workflow, so it gets its own flagged file.
UNRESOLVED_NAME = "UNRESOLVED_referential_gap"

# Column order for the exported event log.
EVENT_EXPORT_COLUMNS = [
    "case_id", "workflow_id", "root_workflow_id", "activity", "previous_activity",
    "execution_index", "timestamp", "execution_time_sec", "token_usage",
    "data_volume_bytes", "pii_detected", "execution_status", "error_type", "system_overhead_sec",
]

# Case-log columns always kept per workflow; everything else is a business attribute that may be empty for a given workflow and is dropped from its file when so.
GENERIC_CASE_COLUMNS = ["case_id", "root_workflow_id", "workflow_name",
                        "start_time", "end_time", "throughput_sec"]

def get_target_engine():
    """Create the SQLAlchemy engine for the target PostgreSQL warehouse."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    db = os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def _env_flag(name: str, default: str) -> bool:
    """Read a boolean environment flag."""
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes")

def _sanitize_filename(name) -> str:
    """Turn an arbitrary workflow name into a safe filename fragment."""
    safe = re.sub(r"[^0-9A-Za-z._-]+", "_", str(name)).strip("_")
    return safe or "workflow"

def _split_paths(output_file: str) -> tuple:
    """Split an output path into (base, extension), defaulting the extension to .csv."""
    base, ext = os.path.splitext(output_file)
    return base, ext or ".csv"

def resolve_workflow_names(engine, workflow_ids) -> dict:
    """Map each root_workflow_id to a safe, human-readable workflow name.

    Reads the raw table once; any id that cannot be resolved keeps its sanitized raw id
    as fallback, so the export never fails on a missing or odd record.
    """
    names = {wid: _sanitize_filename(wid) for wid in workflow_ids}
    if not workflow_ids:
        return names
    try:
        raw = pd.read_sql(f'SELECT DISTINCT "workflowId" AS wid, "workflowData" AS wd FROM {RAW_TABLE}', engine)
        for _, r in raw.iterrows():
            wid = str(r["wid"])
            if wid not in names:
                continue
            wd = r["wd"]
            if isinstance(wd, str):
                try:
                    wd = json.loads(wd)
                except Exception:
                    wd = None
            name = wd.get("name") if isinstance(wd, dict) else None
            if name:
                names[wid] = _sanitize_filename(name)
    except Exception:
        pass
    return names

def iter_workflow_groups(engine, df):
    """Yield (wf_name, group) per root workflow for the split export.

    Resolves human-readable names, de-duplicates name collisions, and honors
    EXPORT_INCLUDE_UNRESOLVED for the referential-gap group.
    """
    real_ids = [w for w in df["root_workflow_id"].dropna().unique().tolist() if w != "unresolved"]
    name_map = resolve_workflow_names(engine, real_ids)
    skip_unresolved = not _env_flag("EXPORT_INCLUDE_UNRESOLVED", "true")

    used_names = {}
    for wf_id, group in df.groupby("root_workflow_id", dropna=False):
        is_unresolved = pd.isna(wf_id) or wf_id == "unresolved"
        if is_unresolved:
            if skip_unresolved:
                continue
            yield UNRESOLVED_NAME, group
            continue
        name = name_map.get(wf_id, _sanitize_filename(wf_id))
        # Guard against two different workflows sharing the same name.
        if name in used_names and used_names[name] != wf_id:
            name = f"{name}_{_sanitize_filename(wf_id)}"
        used_names[name] = wf_id
        yield name, group

def export_event_log(engine, output_file: str) -> int:
    """Export the event log: one file by default, or one per root workflow when split."""
    df = pd.read_sql(f"SELECT * FROM {EVENTS_TABLE} ORDER BY case_id, end_timestamp ASC", engine)
    if df.empty:
        logger.error(f"Export failed: {EVENTS_TABLE} is empty")
        sys.exit(1)

    df["timestamp"] = df["end_timestamp"]  # primary time reference for the Celonis schema
    df = df[[c for c in EVENT_EXPORT_COLUMNS if c in df.columns]]

    if not (_env_flag("EXPORT_SPLIT_BY_WORKFLOW", "false") and "root_workflow_id" in df.columns):
        logger.info(f"Exporting event log to '{output_file}'")
        df.to_csv(output_file, index=False, encoding="utf-8")
        return len(df)

    base, ext = _split_paths(output_file)
    total = 0
    for wf_name, group in iter_workflow_groups(engine, df):
        wf_file = f"{base}_{wf_name}{ext}"
        group.to_csv(wf_file, index=False, encoding="utf-8")
        level = logging.WARNING if wf_name == UNRESOLVED_NAME else logging.INFO
        logger.log(level, f"Wrote {len(group)} events for '{wf_name}' to '{wf_file}'")
        total += len(group)
    return total

def export_case_log(engine, output_file: str) -> int:
    """Export the case log if present. Returns 0 gracefully when the table is missing/empty.

    When split, one file per root workflow is written and business-attribute columns
    that are entirely empty within a workflow are dropped; generic columns are kept.
    """
    if not inspect(engine).has_table(CASES_TABLE):
        return 0
    df = pd.read_sql(f"SELECT * FROM {CASES_TABLE} ORDER BY case_id ASC", engine)
    if df.empty:
        return 0

    if not (_env_flag("EXPORT_SPLIT_BY_WORKFLOW", "false") and "root_workflow_id" in df.columns):
        logger.info(f"Exporting case log to '{output_file}'")
        df.to_csv(output_file, index=False, encoding="utf-8")
        return len(df)

    generic_cols = [c for c in GENERIC_CASE_COLUMNS if c in df.columns]
    base, ext = _split_paths(output_file)
    total = 0
    for wf_name, group in iter_workflow_groups(engine, df):
        business_cols = [c for c in group.columns if c not in generic_cols and group[c].notna().any()]
        group = group[generic_cols + business_cols]
        wf_file = f"{base}_{wf_name}{ext}"
        group.to_csv(wf_file, index=False, encoding="utf-8")
        logger.info(f"Wrote {len(group)} cases for '{wf_name}' to '{wf_file}'")
        total += len(group)
    return total

def export_logs():
    """Read the processed tables and materialize them as CSV files."""
    event_file = os.getenv("EXPORT_PATH", "Event_Logs.csv")
    case_file = os.getenv("CASE_EXPORT_PATH", "Case_Logs.csv")

    try:
        engine = get_target_engine()
        event_count = export_event_log(engine, event_file)
        case_count = export_case_log(engine, case_file)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)

    summary = f"{event_count} events" + (f" and {case_count} cases" if case_count else "")
    logger.info(f"Export finished, materialized {summary}")

if __name__ == "__main__":
    export_logs()