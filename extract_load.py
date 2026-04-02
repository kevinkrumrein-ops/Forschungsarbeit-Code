import os
import pandas as pd
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Konfiguration laden
load_dotenv()

def get_engine(prefix):
    """Erstellt eine SQLAlchemy Engine basierend auf dem Präfix in der .env."""
    user = os.getenv(f"{prefix}_DB_USER")
    pw = os.getenv(f"{prefix}_DB_PASSWORD")
    host = os.getenv(f"{prefix}_DB_HOST")
    port = os.getenv(f"{prefix}_DB_PORT", "5432")
    db = os.getenv(f"{prefix}_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def run_el():
    try:
        # Engines initialisieren
        src_engine = get_engine("N8N")
        tgt_engine = get_engine("TARGET")

        # Tabellen-Mapping: {Quellname: Zielname}
        tables = {
            "execution_entity": "raw_execution_entity",
            "execution_data": "raw_execution_data"
        }

        chunk_size = 5000  # Anzahl der Zeilen pro Durchgang

        for src_table, tgt_table in tables.items():
            print(f"Verarbeite {src_table} -> {tgt_table}...")
            
            # 1:1 Extraktion mit chunksize für Speicher-Effizienz
            first_chunk = True
            total_rows = 0
            
            for df in pd.read_sql(f"SELECT * FROM {src_table}", src_engine, chunksize=chunk_size):
                
                # FIX: Konvertiere komplexe Typen (dict/list) in JSON-Strings
                # Dies verhindert den Fehler "can't adapt type 'dict'"
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

                # Beim ersten Chunk: Tabelle im Ziel ersetzen (replace)
                # Bei weiteren Chunks: Daten anhängen (append)
                mode = "replace" if first_chunk else "append"
                
                df.to_sql(tgt_table, tgt_engine, if_exists=mode, index=False)
                
                total_rows += len(df)
                first_chunk = False
                print(f"  ... {total_rows} Zeilen geladen")

            print(f"Erfolgreich abgeschlossen: {total_rows} Zeilen kopiert.\n")

    except Exception as e:
        print(f"KRITISCHER FEHLER während des ETL-Prozesses:")
        print(f"Details: {str(e)}")
        print("Stelle sicher, dass die Datenbanken (n8n & Target) erreichbar sind.")

if __name__ == "__main__":
    run_el()
