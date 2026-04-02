import os, json, re, pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_db_engine():
    """Erstellt die Verbindung zur DWH-Datenbank auf Port 5434."""
    user, pw, db = os.getenv("TARGET_DB_USER"), os.getenv("TARGET_DB_PASSWORD"), os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@127.0.0.1:5434/{db}")

def format_iso_timestamp(dt):
    """Konvertiert datetime in ISO 8601 (YYYY-MM-DD HH:MM:SS.mmm)."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def get_pointer_val(g, ptr):
    """Universeller Pointer-Zugriff für Listen und Dictionaries (n8n-Graph)."""
    try:
        idx = int(ptr)
        if isinstance(g, list): return g[idx] if 0 <= idx < len(g) else None
        if isinstance(g, dict): return g.get(str(idx)) or g.get(idx)
    except: return None

def resolve_nested(g, val):
    """Löst Pointer-Ketten rekursiv auf."""
    curr = val
    while isinstance(curr, (int, str)) and str(curr).isdigit():
        nxt = get_pointer_val(g, curr)
        if nxt is None: break
        curr = nxt
    return curr

def find_deep_value(g, obj, target_key, visited=None):
    """Durchsucht Dictionaries UND Listen rekursiv nach einem spezifischen Key."""
    if visited is None: visited = set()
    
    # Zirkuläre Referenzen (Endlosschleifen) abfangen
    if isinstance(obj, (int, str)) and str(obj).isdigit():
        if str(obj) in visited: return None
        visited.add(str(obj))
        
    obj = resolve_nested(g, obj)
    
    # 1. Fall: Dictionary -> Suche Key oder durchsuche Values
    if isinstance(obj, dict):
        if target_key in obj: return resolve_nested(g, obj[target_key])
        for val in obj.values():
            result = find_deep_value(g, val, target_key, visited)
            if result is not None: return result
                
    # 2. Fall: Liste -> Durchsuche jedes Element
    elif isinstance(obj, list):
        for item in obj:
            result = find_deep_value(g, item, target_key, visited)
            if result is not None: return result
                
    return None

def extract_events(case_id, data_input):
    """Extrahiert Events mit Deep-Pointer-Resolving für Status, Tokens und Volumen."""
    try:
        g = json.loads(data_input, strict=False) if isinstance(data_input, str) else data_input
        root = get_pointer_val(g, 2)
        if not isinstance(root, dict): return []
        
        mapping = get_pointer_val(g, root.get('runData')) or {}
        events = []
        
        for activity, run_ptr in mapping.items():
            run_list = get_pointer_val(g, run_ptr) or []
            if not isinstance(run_list, list): run_list = [run_list]
                
            for meta_ptr in run_list:
                m = get_pointer_val(g, meta_ptr) or {}
                if not isinstance(m, dict): continue
                
                status = resolve_nested(g, m.get('executionStatus'))
                start_dt = datetime.fromtimestamp(m.get('startTime', 0) / 1000.0)
                dur_ms = m.get('executionTime', 0)
                
                p = resolve_nested(g, m.get('data')) or {}
                tokens = find_deep_value(g, p, 'totalTokens') or find_deep_value(g, p, 'estimatedTokens') or 0
                volume_val = find_deep_value(g, p, 'size') or find_deep_value(g, p, 'fileSize')
                
                if isinstance(volume_val, str) and not str(volume_val).isdigit():
                    nums = re.findall(r'\d+', str(volume_val))
                    volume = int(nums[0]) if nums else len(str(p))
                else: volume = volume_val if volume_val is not None else len(str(p))
                
                events.append({
                    "case_id": case_id, "activity": activity, "start_ts_raw": start_dt,
                    "start_timestamp": format_iso_timestamp(start_dt),
                    "end_timestamp": format_iso_timestamp(start_dt + timedelta(milliseconds=dur_ms)),
                    "execution_time_sec": dur_ms / 1000.0,
                    "token_usage": int(tokens) if str(tokens).isdigit() else 0,
                    "data_volume_bytes": int(volume) if str(volume).isdigit() else 0,
                    "pii_detected": bool(re.search(r"[\w\.-]+@[\w\.-]+", str(p))),
                    "error_type": "None" if status == "success" else str(status)
                })
        return events
    except Exception as e:
        print(f"  [!] Fehler in Case {case_id}: {e}")
        return []

def process_overhead(events):
    """Berechnet system_overhead_sec zwischen aufeinanderfolgenden Nodes."""
    if not events: return []
    events.sort(key=lambda x: x['start_ts_raw'])
    for i in range(len(events)):
        if i == 0: events[i]['system_overhead_sec'] = 0.0
        else:
            prev_end = events[i-1]['start_ts_raw'] + timedelta(seconds=events[i-1]['execution_time_sec'])
            events[i]['system_overhead_sec'] = max(0.0, (events[i]['start_ts_raw'] - prev_end).total_seconds())
    for e in events: del e['start_ts_raw']
    return events

def run_pipeline():
    engine = get_db_engine()
    query = 'SELECT e.id, d.data FROM raw_execution_entity e JOIN raw_execution_data d ON e.id = d."executionId"'
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STARTE PIPELINE: 5434")
    first = True
    for chunk in pd.read_sql(query, engine, chunksize=100):
        all_ev = []
        for _, row in chunk.iterrows():
            all_ev.extend(process_overhead(extract_events(row['id'], row['data'])))
        if all_ev:
            mode = 'replace' if first else 'append'
            pd.DataFrame(all_ev).to_sql('process_mining_events', engine, schema='public', if_exists=mode, index=False)
            print(f"  -> {len(all_ev)} Events ({mode})")
            first = False
    print(f"[{datetime.now().strftime('%H:%M:%S')}] FERTIG. DB: {engine.url.host}:{engine.url.port}/{engine.url.database}")

if __name__ == "__main__": run_pipeline()
