"""
Module 03: Event Log Exporter
-----------------------------
Fetches processed event data from PostgreSQL and exports it to a standardized CSV file.
Prepares the final dataset for seamless, real-time ingestion into Celonis EMS.
"""

import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
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

def export_logs():
    """Extracts processed event logs from the warehouse and materializes them into a CSV file."""
    output_file = os.getenv("EXPORT_PATH", "Event_Logs.csv")
    
    # LINE 1: Clean, jargon-free startup logging
    logger.info(f"[EXPORTER] Exporting processed event logs to '{output_file}'.")

    try:
        engine = get_target_engine()
        
        # Retrieve data using strict process mining sorting constraints (Case ID & Chronological order)
        query = "SELECT * FROM process_mining_events ORDER BY case_id, end_timestamp ASC"
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.error("[EXPORTER] Export failed: Table 'process_mining_events' is empty.")
            sys.exit(1)

        # Map the primary time reference required by the downstream Celonis schema
        df['timestamp'] = df['end_timestamp']

        # Define the exact structural column mapping required for the final event log
        target_columns = [
            'case_id', 'activity', 'previous_activity', 'execution_index',
            'timestamp', 'execution_time_sec', 'token_usage', 'data_volume_bytes', 
            'pii_detected', 'execution_status', 'error_type', 'system_overhead_sec'
        ]
        
        # Filter safely to prevent crashes if schema additions occur upstream in the warehouse
        existing_columns = [col for col in target_columns if col in df.columns]
        df_export = df[existing_columns]

        # Materialize the structured dataset to disk
        df_export.to_csv(output_file, index=False, encoding='utf-8', sep=',')
        
    except Exception as e:
        logger.error(f"[EXPORTER] Critical failure during CSV export: {str(e)}")
        sys.exit(1)

    # LINE 2: Clean outcome logging
    logger.info(f"[EXPORTER] Export finished. Materialized {len(df_export)} events to disk.")

if __name__ == "__main__":
    export_logs()