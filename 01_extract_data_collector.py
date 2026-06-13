"""
Module 01: Data Extraction Collector
------------------------------------
Extracts raw workflow execution data from the n8n REST API using an incremental
delta-load, and stores the raw payloads in PostgreSQL JSONB columns.
"""

import os
import sys
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("EXTRACTOR")

load_dotenv()

RAW_TABLE = "raw_api_executions"

def get_target_engine():
    """Create the SQLAlchemy engine for the target PostgreSQL warehouse."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    db = os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_sync_id(engine) -> int:
    """Return the highest execution ID already stored, or 0 if the table is missing."""
    try:
        with engine.connect() as conn:
            max_id = conn.execute(text(f"SELECT MAX(id) FROM {RAW_TABLE}")).scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0

def create_robust_session() -> requests.Session:
    """Build an HTTP session with automatic retries and exponential backoff."""
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def run_api_extraction():
    """Fetch new executions from the n8n API and append them to the warehouse."""
    api_key = os.getenv("N8N_API_KEY")
    base_url = os.getenv("N8N_API_URL")
    if not api_key or not base_url:
        logger.error("Missing n8n API credentials in environment variables")
        sys.exit(1)
    api_url = f"{base_url}/executions"

    engine = get_target_engine()
    last_id = get_last_sync_id(engine)
    logger.info(f"Fetching new n8n data since execution ID {last_id}")

    session = create_robust_session()
    headers = {"X-N8n-Api-Key": api_key, "Accept": "application/json"}

    next_cursor = None
    total_inserted = 0

    while True:
        params = {"includeData": "true", "limit": 50}
        if next_cursor:
            params["cursor"] = next_cursor

        try:
            response = session.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data", [])
            if not data:
                break

            # The API returns executions newest first, so a page that already contains synced records marks the end of the delta.
            new_data = [item for item in data if int(item["id"]) > last_id]
            if not new_data:
                break

            df = pd.DataFrame(new_data)
            df["id"] = pd.to_numeric(df["id"])

            # Store dict/list columns as native JSONB for efficient downstream parsing.
            json_columns = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, (dict, list))).any()]
            df.to_sql(RAW_TABLE, engine, if_exists="append", index=False,
                      dtype={c: JSONB for c in json_columns})
            total_inserted += len(df)

            # Fewer new records than fetched means we crossed the delta boundary.
            if len(new_data) < len(data):
                break
            next_cursor = payload.get("nextCursor")
            if not next_cursor:
                break
            time.sleep(0.2)
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            sys.exit(1)

    logger.info(f"Extraction finished, appended {total_inserted} new records")

if __name__ == "__main__":
    run_api_extraction()