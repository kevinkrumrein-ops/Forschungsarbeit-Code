import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Initialize environment configuration
load_dotenv()

def get_engine(prefix):
    """
    Factory method to create a database connection engine.
    """
    user = os.getenv(f"{prefix}_DB_USER")
    pw = os.getenv(f"{prefix}_DB_PASSWORD")
    host = os.getenv(f"{prefix}_DB_HOST")
    port = os.getenv(f"{prefix}_DB_PORT", "5432")
    db = os.getenv(f"{prefix}_DB_NAME")
    
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def export_logs():
    """
    Fetches processed event data from PostgreSQL and exports it to a standardized CSV.
    Relies on pre-processed logical ordering from the transformation pipeline.
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-03] [LOAD]      Launching event exporter...")
    
    # Initialize connection to the target analytics database
    engine = get_engine("TARGET")
    
    query = "SELECT * FROM process_mining_events ORDER BY case_id, end_timestamp"
    df = pd.read_sql(query, engine)

    if df.empty:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-03] [ERROR]     Export failed: No data found in 'process_mining_events'.")
        return

    # Standardize timestamp format and select end-point as the primary reference (Milliseconds)
    df['timestamp'] = pd.to_datetime(df['end_timestamp'])

    # Column selection mapping the database schema to the final event log structure
    target_columns = [
        'case_id', 'activity', 'timestamp', 'execution_time_sec', 
        'token_usage', 'data_volume_bytes', 'pii_detected', 
        'execution_status', 'error_type', 'system_overhead_sec'
    ]
    
    df_export = df[target_columns]

    # Dynamic export path configuration via environment variables
    output_file = os.getenv("EXPORT_PATH", "Event_Logs.csv")
    
    # Export to CSV using UTF-8 encoding and enforcing a strict datetime format 
    df_export.to_csv(output_file, index=False, encoding='utf-8', sep=',')

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-03] [SUCCESS]   Exported {len(df_export)} events to '{output_file}'.")

if __name__ == "__main__":
    try:
        export_logs()
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-03] [ERROR]     CRITICAL ERROR during CSV export: {e}")