import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Datenbank-Anbindung: Lade Umgebungsvariablen
load_dotenv()

USER = os.getenv('TARGET_DB_USER')
PASSWORD = os.getenv('TARGET_DB_PASSWORD')
DB_NAME = os.getenv('TARGET_DB_NAME')
HOST = os.getenv('TARGET_DB_HOST', '127.0.0.1')
PORT = os.getenv('TARGET_DB_PORT', '5434')

# Erstelle SQLAlchemy-Engine
connection_string = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def export_logs():
    # Lade die Daten aus der Tabelle
    query = "SELECT * FROM process_mining_events"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("Keine Daten in der Tabelle 'process_mining_events' gefunden.")
        return

    # 2. Daten-Transformation:

    # Mapping: end_timestamp zu timestamp umbenennen
    # Wir stellen sicher, dass timestamp-Spalten als datetime-Objekte vorliegen
    df['end_timestamp'] = pd.to_datetime(df['end_timestamp'])
    df['timestamp'] = df['end_timestamp']

    # Sortierung nach case_id und timestamp
    df = df.sort_values(by=['case_id', 'timestamp'])

    # Zeitgleichheit innerhalb eines Cases behandeln (+1ms auf Duplikate)
    # Wir gruppieren nach case_id und addieren für jeden identischen Zeitstempel innerhalb der Gruppe Millisekunden
    def adjust_timestamps(group):
        # cumcount() zählt das Vorkommen jedes Zeitstempels innerhalb der Gruppe (0, 1, 2...)
        # Wir addieren diese Anzahl in Millisekunden auf den Zeitstempel
        group['timestamp'] = group['timestamp'] + pd.to_timedelta(group.groupby('timestamp').cumcount(), unit='ms')
        return group

    df = df.groupby('case_id', group_keys=False).apply(adjust_timestamps)

    # Spalten-Auswahl gemäß Vorgabe (exakt wie in der DB, außer timestamp)
    target_columns = [
        'case_id', 'activity', 'timestamp', 'execution_time_sec', 
        'token_usage', 'data_volume_bytes', 'pii_detected', 
        'execution_status', 'error_type', 'system_overhead_sec'
    ]
    
    # Filtere nur die gewünschten Spalten
    df_export = df[target_columns]

    # 3. Output: Speichere als Event_Logs.csv
    output_file = 'Event_Logs.csv'
    df_export.to_csv(output_file, index=False, encoding='utf-8', sep=',')

    print(f"Export erfolgreich: {len(df_export)} Zeilen wurden in '{output_file}' gespeichert.")

if __name__ == "__main__":
    try:
        export_logs()
    except Exception as e:
        print(f"Fehler beim Export: {e}")
