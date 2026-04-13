import os
import pandas as pd
import json
from sqlalchemy import create_engine
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

def run_el():
    """
    Executes the Extract-Load (EL) process. 
    Synchronizes raw execution data from n8n to the analytics database.
    """
    try:
        src_engine = get_engine("N8N")
        tgt_engine = get_engine("TARGET")

        tables = {
            "execution_entity": "raw_execution_entity",
            "execution_data": "raw_execution_data"
        }

        chunk_size = 5000 

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [EXTRACT]   Launching data collection...")

        for src_table, tgt_table in tables.items():
            first_chunk = True
            total_rows = 0
            
            # Use chunking to maintain a low memory footprint during large data transfers
            for df in pd.read_sql(f"SELECT * FROM {src_table}", src_engine, chunksize=chunk_size):
                
                # Data Serialization: Convert nested Python objects (dict/list) to JSON strings. (Prevents 'adapter type' errors during the SQL bulk insert.)
                json_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
                for col in json_columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

                # Atomicity handling: Replace table on first write, append subsequent chunks.
                mode = "replace" if first_chunk else "append"
                df.to_sql(tgt_table, tgt_engine, if_exists=mode, index=False, method='multi')
                
                total_rows += len(df)
                first_chunk = False

            # Log progress for each completed table
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [PROGRESS]  Transferring '{src_table}' -> {total_rows} rows.")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [SUCCESS]   Extraction completed.")

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [PHASE-01] [ERROR]     ETL FAILURE: {str(e)}")

if __name__ == "__main__":
    run_el()