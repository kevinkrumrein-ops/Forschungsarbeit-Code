"""
Module 01: Data Extraction Collector
------------------------------------
Extracts raw workflow execution data from the n8n REST API.
Implements an incremental delta-load strategy to optimize network I/O and 
utilizes PostgreSQL's native JSONB data type for high-performance downstream querying.
"""

import os
import time
import json
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
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

load_dotenv()

def get_target_engine():
    """Initializes and returns the SQLAlchemy engine for the Data Warehouse."""
    user = os.getenv('TARGET_DB_USER')
    pw = os.getenv('TARGET_DB_PASSWORD')
    host = os.getenv('TARGET_DB_HOST', '127.0.0.1')
    port = os.getenv('TARGET_DB_PORT', '5434')
    db = os.getenv('TARGET_DB_NAME')
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_sync_id(engine):
    """Retrieves the highest execution ID currently stored to facilitate Delta Load."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(id) FROM raw_api_executions'))
            max_id = result.scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0

def create_robust_session():
    """Creates an HTTP session with built-in retry logic and exponential backoff."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def run_api_extraction():
    """Main orchestrator for the API extraction process."""
    api_key = os.getenv('N8N_API_KEY')
    if not api_key:
        logging.error("CRITICAL: N8N_API_KEY is missing from environment variables.")
        return

    tgt_engine = get_target_engine()
    last_id = get_last_sync_id(tgt_engine)
    logging.info(f"Initiating API extraction. Delta load active (Fetching IDs > {last_id}).")

    session = create_robust_session()
    api_url = f"{os.getenv('N8N_API_URL')}/executions"
    headers = {"X-N8n-Api-Key": api_key, "Accept": "application/json"}
    
    next_cursor = None
    total_extracted = 0
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
            
            # DELTA LOAD LOGIC
            new_data = [item for item in data if int(item['id']) > last_id]
            
            if not new_data:
                logging.info("Historical sync threshold reached. No newer executions found.")
                break
                
            df = pd.DataFrame(new_data)
            
            json_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
            db_dtypes = {col: JSONB for col in json_columns}
            
            # Insert the chunk into the target Data Warehouse
            df.to_sql(target_table, tgt_engine, if_exists="append", index=False, dtype=db_dtypes)
            
            total_extracted += len(df)
            logging.info(f"Chunk persisted. Inserted {len(df)} executions (Total: {total_extracted}).")
            
            if len(new_data) < len(data):
                break
                
            next_cursor = payload.get('nextCursor')
            if not next_cursor:
                break
                
            time.sleep(0.2) 

        except Exception as e:
            logging.error(f"FATAL EXTRACTION ERROR: {str(e)}")
            break

if __name__ == "__main__":
    run_api_extraction()