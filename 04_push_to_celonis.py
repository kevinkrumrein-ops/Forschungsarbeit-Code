"""
Module 04: Celonis EMS Ingestion
--------------------------------
Pushes the per-workflow exports from Module 03 into Celonis (EMS) via PyCelonis.

For each workflow the script (a) full-replaces its event and case tables in the
data pool, (b) adds both to the workflow's own data model, (c) links them 1:N on
case_id (case = 1 side, event = N side), and (d) sets the process configuration.
Giving each workflow its own data model keeps different n8n workflows from being
merged into one process model.
"""

import os
import re
import sys
import glob
import logging
import pandas as pd
from dotenv import load_dotenv
from pycelonis import get_celonis

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CELONIS")
logging.getLogger("pycelonis").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv()

# Datetime columns per log type, so PyCelonis provisions native DATETIME columns.
EVENT_DATETIME_COLUMNS = ["timestamp"]
CASE_DATETIME_COLUMNS = ["start_time", "end_time"]

# Process-mining column contract (set by Modules 02a/02b).
CASE_ID_COLUMN = "case_id"
ACTIVITY_COLUMN = "activity"
TIMESTAMP_COLUMN = "timestamp"

def _safe_identifier(label) -> str:
    """Turn a workflow label into a safe lowercase table identifier fragment."""
    safe = re.sub(r"[^0-9A-Za-z]+", "_", str(label)).strip("_").lower()
    return safe or "workflow"

def _short_error(e) -> str:
    """One-line error summary; never dumps the raw HTTP request/response or cookies."""
    lines = [ln.strip() for ln in str(e).splitlines() if ln.strip()]
    if not lines:
        return type(e).__name__
    # Prefer the line carrying the API error message over the request/header dump.
    for ln in lines:
        if "error" in ln.lower():
            return ln[:200]
    return lines[0][:200]

def discover_workflows(event_csv: str, case_csv: str) -> list:
    """Find the per-workflow exports from Module 03.

    Returns a list of (label, event_path, case_path_or_None). Falls back to the single
    base files when the split is disabled. UNRESOLVED exports are excluded.
    """
    event_base, ext = os.path.splitext(event_csv)
    case_base, _ = os.path.splitext(case_csv)
    export_dir = os.path.dirname(event_csv) or "."

    split_files = sorted(glob.glob(os.path.join(export_dir, f"{os.path.basename(event_base)}_*{ext}")))
    workflows = []

    if split_files:
        for ev_path in split_files:
            label = os.path.basename(ev_path)[len(os.path.basename(event_base)) + 1: -len(ext)]
            if label.upper().startswith("UNRESOLVED"):
                logger.info(f"Skipping referential-gap export '{os.path.basename(ev_path)}'")
                continue
            case_path = f"{case_base}_{label}{ext}"
            workflows.append((label, ev_path, case_path if os.path.exists(case_path) else None))
    elif os.path.exists(event_csv):
        # Split disabled: treat the single export as one workflow.
        workflows.append(("n8n", event_csv, case_csv if os.path.exists(case_csv) else None))

    return workflows

def upload_table(data_pool, csv_path: str, table_name: str, datetime_columns=None) -> int:
    """Read a CSV and full-replace the corresponding table in the data pool."""
    df = pd.read_csv(csv_path)
    for col in (datetime_columns or []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    data_pool.create_table(df, table_name=table_name, drop_if_exists=True, force=True)
    return len(df)

def get_or_create_data_model(data_pool, name: str):
    """Resolve the workflow's data model, creating it if it does not exist."""
    try:
        return data_pool.get_data_models().find(name)
    except Exception:
        return data_pool.create_data_model(name)

def _get_or_add_table(model, table_name: str):
    """Return (table, created): the model table for this alias, added from the pool if absent."""
    try:
        return model.get_tables().find(table_name), False
    except Exception:
        return model.add_table(name=table_name, alias=table_name), True

def configure_data_model(data_pool, model_name: str, event_table: str, case_table):
    """Add the workflow's tables to its data model, link them 1:N on case_id, and set
    the process configuration. Idempotent: a model already wired from a prior run is
    left untouched. Any modelling-API error is logged as one line, not fatal.
    """
    try:
        model = get_or_create_data_model(data_pool, model_name)

        ev_tbl, ev_new = _get_or_add_table(model, event_table)
        ca_tbl, ca_new = _get_or_add_table(model, case_table) if case_table else (None, False)

        # If both tables already existed in the model, it was wired on a previous run.
        # The pool tables were replaced in place, so the existing keys/config still hold.
        if not (ev_new or ca_new):
            logger.info(f"Data model '{model_name}' already configured")
            return

        if ca_tbl is not None:
            # 1:N foreign key on case_id (case = "1" side, event = "N" side).
            model.create_foreign_key(
                source_table_id=ca_tbl.id,
                target_table_id=ev_tbl.id,
                columns=[(CASE_ID_COLUMN, CASE_ID_COLUMN)],
            )

        model.create_process_configuration(
            activity_table_id=ev_tbl.id,
            case_id_column=CASE_ID_COLUMN,
            activity_column=ACTIVITY_COLUMN,
            timestamp_column=TIMESTAMP_COLUMN,
            case_table_id=(ca_tbl.id if ca_tbl is not None else None),
        )

        model.reload(wait=True)
        logger.info(f"Data model '{model_name}' configured (1:N on {CASE_ID_COLUMN})")
    except Exception as e:
        logger.warning(f"Tables for '{model_name}' uploaded, but data-model setup failed: {_short_error(e)}")

def push_to_celonis():
    """Upload every discovered workflow export to Celonis and configure its data model."""
    celonis_url = os.getenv("CELONIS_URL")
    api_token = os.getenv("CELONIS_API_TOKEN")
    pool_name = os.getenv("CELONIS_DATA_POOL_NAME", "n8n Agent Process Mining")
    event_csv = os.getenv("EXPORT_PATH", "Event_Logs.csv")
    case_csv = os.getenv("CASE_EXPORT_PATH", "Case_Logs.csv")

    if not celonis_url or not api_token or api_token == "DEIN_KOPIERTER_PERSONAL_API_KEY":
        logger.error("Ingestion aborted: missing or unconfigured Celonis API credentials")
        sys.exit(1)

    workflows = discover_workflows(event_csv, case_csv)
    if not workflows:
        logger.error("Ingestion aborted: no event-log exports found (run Module 03 first)")
        sys.exit(1)

    logger.info(f"Uploading {len(workflows)} workflow(s) to data pool '{pool_name}'")

    try:
        celonis = get_celonis(base_url=celonis_url, api_token=api_token, key_type="USER_KEY")
        try:
            data_pool = celonis.data_integration.get_data_pools().find(pool_name)
        except Exception:
            data_pool = celonis.data_integration.create_data_pool(name=pool_name)
    except Exception as e:
        logger.error(f"Connection failed: {_short_error(e)}")
        sys.exit(1)

    total_events = 0
    for label, ev_path, ca_path in workflows:
        safe = _safe_identifier(label)
        event_table = f"event_log_{safe}"
        case_table = f"case_log_{safe}" if ca_path else None

        # The event table is mandatory; skip the whole workflow if it fails to upload.
        try:
            ev_rows = upload_table(data_pool, ev_path, event_table, EVENT_DATETIME_COLUMNS)
        except Exception as e:
            logger.error(f"Failed to upload event log for '{label}': {_short_error(e)}")
            continue

        ca_rows = 0
        if ca_path:
            try:
                ca_rows = upload_table(data_pool, ca_path, case_table, CASE_DATETIME_COLUMNS)
            except Exception as e:
                logger.error(f"Failed to upload case log for '{label}': {_short_error(e)}")
                case_table = None  # do not model a table that failed to upload

        configure_data_model(data_pool, f"n8n - {label}", event_table, case_table)

        logger.info(f"'{label}': {ev_rows} events"
                    + (f", {ca_rows} cases" if ca_rows else "") + " synchronized")
        total_events += ev_rows

    logger.info(f"Ingestion finished, {len(workflows)} workflow(s), {total_events} events total")

if __name__ == "__main__":
    push_to_celonis()