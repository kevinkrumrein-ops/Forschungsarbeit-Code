"""
Module 04: Celonis EMS Ingestion Pipeline
----------------------------------------
Automates the synchronization of the standardized Process Mining Event Log (CSV)
with the Celonis Execution Management System (EMS) via the cloud REST API.

The mandatory activity table is always uploaded. A case table is uploaded in addition
if a 'Case_Logs.csv' file is present from a workflow-specific case-builder script.
"""

import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
from pycelonis import get_celonis

# Uniform logging configuration matching the pipeline architecture
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("CELONIS")

# Muffle chatty third-party HTTP and API framework connection logs
logging.getLogger("pycelonis").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv()

EVENT_TABLE = "event_log_n8n"
CASE_TABLE  = "case_log_n8n"

def upload_table(data_pool, csv_path: str, table_name: str, datetime_columns: list = None) -> int:
    """Reads a CSV file and replaces the corresponding table in the Celonis data pool."""
    df = pd.read_csv(csv_path)

    # Cast text timestamps back into datetime objects so PyCelonis provisions
    # native DATETIME columns instead of plain strings.
    for col in (datetime_columns or []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    data_pool.create_table(df, table_name=table_name, drop_if_exists=True, force=True)
    return len(df)

def push_to_celonis():
    """Streams the local CSV exports into the Celonis cloud data integration layer."""
    celonis_url = os.getenv("CELONIS_URL")
    api_token   = os.getenv("CELONIS_API_TOKEN")
    pool_name   = os.getenv("CELONIS_DATA_POOL_NAME", "n8n Agent Process Mining")
    event_csv   = os.getenv("EXPORT_PATH",      "Event_Logs.csv")
    case_csv    = os.getenv("CASE_EXPORT_PATH", "Case_Logs.csv")

    # Guard Clause: Verify API credential integrity
    if not celonis_url or not api_token or api_token == "DEIN_KOPIERTER_PERSONAL_API_KEY":
        logger.error("[CELONIS] Ingestion aborted: Missing or unconfigured Celonis API credentials.")
        sys.exit(1)

    # Guard Clause: Event log file is mandatory; case log is optional
    if not os.path.exists(event_csv):
        logger.error(f"[CELONIS] Ingestion aborted: Source file '{event_csv}' missing. Run Module 03 first.")
        sys.exit(1)

    logger.info(f"[CELONIS] Uploading local exports to Data Pool '{pool_name}'.")

    try:
        # Establish connection with the Celonis cloud instance
        celonis = get_celonis(base_url=celonis_url, api_token=api_token, key_type="USER_KEY")
        data_pools = celonis.data_integration.get_data_pools()

        # Dynamic Data Pool resolution or fallback initialization
        try:
            data_pool = data_pools.find(pool_name)
        except Exception:
            data_pool = celonis.data_integration.create_data_pool(name=pool_name)

    except Exception as e:
        logger.error(f"[CELONIS] Connection failure: {str(e)}")
        sys.exit(1)

    # Push the mandatory activity table; abort the pipeline if this fails
    try:
        event_rows = upload_table(data_pool, event_csv, EVENT_TABLE, datetime_columns=['timestamp'])
    except Exception as e:
        logger.error(f"[CELONIS] Failed to upload event log: {str(e)}")
        sys.exit(1)

    # Push the optional case table; log and continue if it fails (events are already up)
    case_rows = 0
    if os.path.exists(case_csv):
        try:
            case_rows = upload_table(
                data_pool, case_csv, CASE_TABLE,
                datetime_columns=['case_start_timestamp', 'case_end_timestamp', 'invoice_date']
            )
        except Exception as e:
            logger.error(f"[CELONIS] Failed to upload case log: {str(e)}")

    summary = f"{event_rows} events" + (f" and {case_rows} cases" if case_rows else "")
    logger.info(f"[CELONIS] Ingestion finished. Synchronized {summary}.")

if __name__ == "__main__":
    push_to_celonis()