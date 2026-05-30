"""
Module 03: Event Log Exporter
-----------------------------
Fetches processed event data from PostgreSQL and exports it to a standardized CSV file.
Prepares the final dataset for seamless, real-time ingestion into Celonis EMS.

The mandatory activity table is always exported. A case table is exported in addition
if an optional workflow-specific case builder has populated 'process_mining_cases'.
"""

import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect
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

def export_event_log(engine, output_file: str) -> int:
    """Materializes the mandatory activity table into a Celonis-ready CSV."""
    # Retrieve data using strict process mining sorting constraints (Case ID & Chronological order)
    query = "SELECT * FROM process_mining_events ORDER BY case_id, end_timestamp ASC"
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.error("[EXPORTER] Export failed: Table 'process_mining_events' is empty.")
        sys.exit(1)

    # Map the primary time reference required by the downstream Celonis schema
    df['timestamp'] = df['end_timestamp']

    target_columns = [
        'case_id', 'activity', 'previous_activity', 'execution_index',
        'timestamp', 'execution_time_sec', 'token_usage', 'data_volume_bytes',
        'pii_detected', 'execution_status', 'error_type', 'system_overhead_sec'
    ]
    existing_columns = [col for col in target_columns if col in df.columns]
    df_export = df[existing_columns]

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
