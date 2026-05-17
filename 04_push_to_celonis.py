"""
Module 04: Celonis EMS Ingestion Pipeline
----------------------------------------
Automates the synchronization of the standardized Process Mining Event Log (CSV)
with the Celonis Execution Management System (EMS) via the cloud REST API.
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

def push_to_celonis():
    """Streams the local CSV event log into the Celonis cloud data integration layer."""
    celonis_url = os.getenv("CELONIS_URL")
    api_token = os.getenv("CELONIS_API_TOKEN")
    pool_name = os.getenv("CELONIS_DATA_POOL_NAME", "n8n Agent Process Mining")
    csv_path = os.getenv("EXPORT_PATH", "Event_Logs.csv")
    table_name = "event_log_n8n"

    # Guard Clause: Verify API credential integrity
    if not celonis_url or not api_token or api_token == "DEIN_KOPIERTER_PERSONAL_API_KEY":
        logger.error("[CELONIS] Ingestion aborted: Missing or unconfigured Celonis API credentials.")
        sys.exit(1)

    # Guard Clause: Verify upstream file existence
    if not os.path.exists(csv_path):
        logger.error(f"[CELONIS] Ingestion aborted: Source file '{csv_path}' missing. Run Module 03 first.")
        sys.exit(1)

    # LINE 1: Clean, jargon-free startup logging
    logger.info(f"[CELONIS] Uploading local event log to Data Pool '{pool_name}'.")

    try:
        df = pd.read_csv(csv_path)
        
        # Explicitly cast the text timestamp back to a datetime object.
        # This enforces PyCelonis to provision a true DATETIME database column.
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Establish connection with the Celonis cloud instance
        celonis = get_celonis(base_url=celonis_url, api_token=api_token, key_type="USER_KEY")
        data_pools = celonis.data_integration.get_data_pools()
        
        # Dynamic Data Pool resolution or fallback initialization
        try:
            data_pool = data_pools.find(pool_name)
        except Exception:
            data_pool = celonis.data_integration.create_data_pool(name=pool_name)

        # Recreate schema and execute high-performance bulk data push
        data_pool.create_table(df, table_name=table_name, drop_if_exists=True, force=True)

    except Exception as e:
        logger.error(f"[CELONIS] Critical failure during cloud ingestion: {str(e)}")
        sys.exit(1)

    # LINE 2: Clean outcome logging
    logger.info(f"[CELONIS] Ingestion finished. Synchronized {len(df)} rows to table '{table_name}'.")

if __name__ == "__main__":
    push_to_celonis()