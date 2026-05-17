"""
Module 01: Data Extraction Collector
------------------------------------
Extracts raw workflow execution data from the n8n REST API.
Implements an incremental delta-load strategy and stores payloads 
directly into PostgreSQL JSONB columns for optimal downstream parsing.
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

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("EXTRACTOR")

load_dotenv()

def get_target_engine():
    """Initializes and returns the SQLAlchemy connection engine for PostgreSQL."""
    user = os.getenv('TARGET_DB_USER')
    pw = os.getenv('TARGET_DB_PASSWORD')
    host = os.getenv('TARGET_DB_HOST', '127.0.0.1')
    port = os.getenv('TARGET_DB_PORT', '5434')
    db = os.getenv('TARGET_DB_NAME')
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_sync_id(engine) -> int:
    """Retrieves the highest existing execution ID from the warehouse to ensure a true Delta Load."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(id) FROM raw_api_executions'))
            max_id = result.scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0  # Fallback to 0 if table does not exist yet

def create_robust_session() -> requests.Session:
    """Configures a resilient HTTP session with automated retries and exponential backoff."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def run_api_extraction():
    """Orchestrates the incremental REST API extraction and loads data into the warehouse."""
    api_key = os.getenv('N8N_API_KEY')
    api_url = f"{os.getenv('N8N_API_URL')}/executions" if os.getenv('N8N_API_URL') else None

    if not api_key or not api_url:
        logger.error("[EXTRACTOR] Missing N8N API credentials or configuration in environment variables.")
        sys.exit(1)

    tgt_engine = get_target_engine()
    last_id = get_last_sync_id(tgt_engine)
    
    # LINE 1: Minimalist startup log showing the current state
    logger.info(f"[EXTRACTOR] Fetching new n8n data since execution ID {last_id}.")

    session = create_robust_session()
    headers = {"X-N8n-Api-Key": api_key, "Accept": "application/json"}
    
    next_cursor = None
    total_inserted = 0
    target_table = "raw_api_executions"
    
    while True:
        params = {"includeData": "true", "limit": 50}
        if next_cursor:
            params["cursor"] = next_cursor
            
        try:
            response = session.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            payload = response.json()
            data = payload.get('data', [])
            
            if not data:
                break
            
            # Filter for new records strictly greater than our last synced ID
            new_data = [item for item in data if int(item['id']) > last_id]
            
            if not new_data:
                break 
                
            df = pd.DataFrame(new_data)
            df['id'] = pd.to_numeric(df['id'])
            
            # Automatically detect dictionary/list columns and flag them as PostgreSQL JSONB datatype
            json_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
            db_dtypes = {col: JSONB for col in json_columns}
            
            # Bulk upsert current chunk into the Target Data Warehouse
            df.to_sql(target_table, tgt_engine, if_exists="append", index=False, dtype=db_dtypes)
            total_inserted += len(df)
            
            # If the API returned fewer new records than the limit, the full delta has been synced
            if len(new_data) < len(data):
                break
                
            next_cursor = payload.get('nextCursor')
            if not next_cursor:
                break
                
            time.sleep(0.2) 

        except Exception as e:
            logger.error(f"[EXTRACTOR] Fatal runtime error during extraction: {str(e)}")
            sys.exit(1)

    # LINE 2: Minimalist outcome log
    logger.info(f"[EXTRACTOR] Extraction finished. Appended {total_inserted} new records to database.")

if __name__ == "__main__":
    run_api_extraction()