import os
import pandas as pd
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# Initialize environment variables
load_dotenv()

def get_engine(prefix):
    """
    Factory function to create a SQLAlchemy engine using environment-specific prefixes.
    Ensures a standardized connection string for both source and target databases.
    """
    user = os.getenv(f"{prefix}_DB_USER")
    pw = os.getenv(f"{prefix}_DB_PASSWORD")
    host = os.getenv(f"{prefix}_DB_HOST")
    port = os.getenv(f"{prefix}_DB_PORT", "5432")
    db = os.getenv(f"{prefix}_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_sync_id(target_engine):
    """
    Fetches the highest execution ID from the target Data Warehouse to enable incremental delta loads.
    If the table does not exist or is empty, defaults to 0 for a full initial load.
    """
    try:
        with target_engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(id) FROM raw_execution_entity'))
            max_id = result.scalar()
            return max_id if max_id is not None else 0
    except Exception:
        return 0

def run_el():
    """
    Executes the Extract-Load (EL) process. 
    Synchronizes raw execution data from n8n to the analytics database.
    """
    try:
        src_engine = get_engine("N8N")
        tgt_engine = get_engine("TARGET")

        last_id = get_last_sync_id(tgt_engine)
        print(f"Letzte synchronisierte ID: {last_id}")

        # Define explicit SQL queries to enforce delta loads and filter for completed/crashed executions only. (No Running executions to avoid partial data capture.)
        queries = {
            "raw_execution_entity": f"""
                SELECT * FROM execution_entity 
                WHERE id > {last_id} 
                AND ("stoppedAt" IS NOT NULL OR status = 'crashed')
            """,
            "raw_execution_data": f"""
                SELECT d.* FROM execution_data d
                JOIN execution_entity e ON d."executionId" = e.id
                WHERE e.id > {last_id} 
                AND (e."stoppedAt" IS NOT NULL OR e.status = 'crashed')
            """
        }

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [EXTRACT]   Launching data collection...")

        for tgt_table, query in queries.items():
            
            df = pd.read_sql(query, src_engine)
            
            if df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [PROGRESS]  Transferring '{tgt_table}' -> 0 rows.")
                continue
                
            # Data Serialization: Convert nested Python objects (dict/list) to JSON strings.
            json_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
            for col in json_columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

            # Atomicity handling: Append to perform an incremental delta load.
            df.to_sql(tgt_table, tgt_engine, if_exists="append", index=False, method='multi')
            
            total_rows = len(df)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [PROGRESS]  Transferring '{tgt_table}' -> {total_rows} rows.")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [SUCCESS]   Extraction completed.")

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [ERROR]     ETL FAILURE: {str(e)}")

if __name__ == "__main__":
    run_el()