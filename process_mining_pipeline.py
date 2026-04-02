import os
import json
import re
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Konfiguration laden
load_dotenv()

def get_db_engine():
    """Erstellt die Verbindung zur DWH-Datenbank auf Port 5434."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    db = os.getenv("TARGET_DB_NAME")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = 5434
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def format_iso_timestamp(dt):
    """Konvertiert datetime in ISO 8601 (YYYY-MM-DD HH:MM:SS.mmm)."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def get_pointer_val(g, ptr):
    """Universeller Pointer-Zugriff für Listen und Dictionaries (n8n-Graph)."""
    try:
        if ptr is None: return None
        idx = int(ptr)
        if isinstance(g, list):
            return g[idx] if 0 <= idx < len(g) else None
        elif isinstance(g, dict):
            return g.get(str(idx)) or g.get(idx)
        return None
    except:
        return None

def extract_events(case_id, data_input):
    """Extrahiert Events basierend auf der Pointer-Logik: g[2] -> runData -> mapping."""
    try:
        g = json.loads(data_input, strict=False) if isinstance(data_input, str) else data_input
        
        # Einstiegspunkt Index 2 (runData)
        root_obj = get_pointer_val(g, 2)
        if not isinstance(root_obj, dict): return []
        
        run_data_ptr = root_obj.get('runData')
        mapping = get_pointer_val(g, run_data_ptr) or {}
        events = []
        
        for activity, run_ptr in mapping.items():
            run_list = get_pointer_val(g, run_ptr) or []
            if not isinstance(run_list, list): run_list = [run_list]
                
            for meta_ptr in run_list:
                m = get_pointer_val(g, meta_ptr) or {}
                if not isinstance(m, dict): continue
                
                # Zeit-Extraktion
                start_ms = m.get('startTime', 0)
                dur_ms = m.get('executionTime', 0)
                start_dt = datetime.fromtimestamp(start_ms / 1000.0)
                
                # Metriken (Tokens & Volumen)
                p = get_pointer_val(g, m.get('data')) or {}
                tokens = p.get('tokenUsage', {}).get('total', 0) if isinstance(p, dict) else 0
                volume = p.get('fileSize') if isinstance(p, dict) and 'fileSize' in p else len(str(p))
                
                events.append({
                    "case_id": case_id,
                    "activity": activity,
                    "start_ts_raw": start_dt, # Intern für Overhead
                    "start_timestamp": format_iso_timestamp(start_dt),
                    "end_timestamp": format_iso_timestamp(start_dt + timedelta(milliseconds=dur_ms)),
                    "execution_time_sec": dur_ms / 1000.0,
                    "token_usage": tokens,
                    "data_volume_bytes": volume,
                    "pii_detected": bool(re.search(r"[\w\.-]+@[\w\.-]+", str(p))),
                    "error_type": "None" if m.get('executionStatus') == "success" else m.get('executionStatus')
                })
        return events
    except Exception as e:
        print(f"  [!] Fehler in Case {case_id}: {str(e)}")
        return []

def process_overhead(events):
    """Berechnet system_overhead_sec zwischen aufeinanderfolgenden Nodes."""
    if not events: return []
    events.sort(key=lambda x: x['start_ts_raw'])
    
    for i in range(len(events)):
        if i == 0:
            events[i]['system_overhead_sec'] = 0.0
        else:
            prev_end = events[i-1]['start_ts_raw'] + timedelta(seconds=events[i-1]['execution_time_sec'])
            diff = (events[i]['start_ts_raw'] - prev_end).total_seconds()
            events[i]['system_overhead_sec'] = max(0.0, diff)
            
    for e in events: del e['start_ts_raw']
    return events

def run_pipeline():
    engine = get_db_engine()
    query = 'SELECT e.id, d.data FROM raw_execution_entity e JOIN raw_execution_data d ON e.id = d."executionId"'
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STARTE PIPELINE: {engine.url.host}:{engine.url.port}")
    first_chunk = True
    
    for chunk in pd.read_sql(query, engine, chunksize=100):
        all_events = []
        for _, row in chunk.iterrows():
            events = extract_events(row['id'], row['data'])
            all_events.extend(process_overhead(events))
        
        if all_events:
            df = pd.DataFrame(all_events)
            mode = 'replace' if first_chunk else 'append'
            df.to_sql('process_mining_events', engine, schema='public', if_exists=mode, index=False)
            print(f"  -> {len(all_events)} Events geladen (Modus: {mode})")
            first_chunk = False

    print(f"[{datetime.now().strftime('%H:%M:%S')}] ERFOLGREICH ABGESCHLOSSEN.")

if __name__ == "__main__":
    run_pipeline()
