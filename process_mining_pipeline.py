import os, json, re, pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# n8n Core-Blacklist: Diese Keys führen oft zu globalen Daten-Lecks (Trigger-Problem)
FORBIDDEN_KEYS = {'workflowData', 'sourceOverwrite', 'pairedItem', 'parent', 'context', 'chatHistory'}
# Globale System-Pointer: Wenn ein Pointer auf diese Indizes zeigt, ist es der Workflow-Root (Gefahr!)
SYSTEM_INDICES = {'0', '1', '2', '3', '4'}

def get_db_engine():
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    db = os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@127.0.0.1:5434/{db}")

def format_iso_timestamp(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def get_pointer_val(g, ptr):
    try:
        idx = int(ptr)
        if isinstance(g, list): return g[idx] if 0 <= idx < len(g) else None
        return None
    except: return None

def smart_resolve(g, obj, visited=None, depth=0):
    """
    Löst Pointer auflösen, bleibt aber im Node-Kontext.
    Wichtig für korrektes Datenvolumen und PII-Erkennung.
    """
    if visited is None: visited = set()
    if depth > 20: return "[Max Depth]" 

    if isinstance(obj, (int, str)) and str(obj).isdigit():
        ptr_str = str(obj)
        if ptr_str in SYSTEM_INDICES or ptr_str in visited:
            return obj 
        
        visited.add(ptr_str)
        resolved = get_pointer_val(g, obj)
        if resolved is None: return obj 
        return smart_resolve(g, resolved, visited, depth + 1)
    
    if isinstance(obj, dict):
        return {k: smart_resolve(g, v, visited.copy(), depth + 1) 
                for k, v in obj.items() if k not in FORBIDDEN_KEYS}
    
    if isinstance(obj, list):
        return [smart_resolve(g, i, visited.copy(), depth + 1) for i in obj]
    
    return obj

def extract_tokens_targeted(g, obj, visited=None):
    """
    Sucht gezielt nach Tokens. Nutzt die chirurgische Logik (erster Treffer),
    um Doppelzählung (z.B. prompt + total) zu verhindern.
    """
    if visited is None: visited = set()
    
    if isinstance(obj, (int, str)) and str(obj).isdigit():
        ptr_str = str(obj)
        if ptr_str in visited or int(ptr_str) < 5: return 0
        visited.add(ptr_str)
        resolved = get_pointer_val(g, obj)
        if resolved: return extract_tokens_targeted(g, resolved, visited)
        return 0

    if isinstance(obj, dict):
        # Priorität: Wenn 'totalTokens' da ist, nimm es sofort und stoppe für diesen Zweig.
        # Das verhindert, dass wir promptTokens (520) + totalTokens (520) zu 1040 addieren.
        for k in ['totalTokens', 'estimatedTokens', 'tokenUsage']:
            if k in obj:
                try: return int(obj[k])
                except: pass
        
        # Fallback: Wenn kein 'total' da ist, suche in Unter-Objekten
        for k, v in obj.items():
            if k in FORBIDDEN_KEYS: continue
            res = extract_tokens_targeted(g, v, visited)
            if res > 0: return res
            
    elif isinstance(obj, list):
        for item in obj:
            res = extract_tokens_targeted(g, item, visited)
            if res > 0: return res
    return 0

def contains_pii(text):
    """Scannt auf alle 5 für die Masterarbeit relevanten PII-Kategorien."""
    patterns = {
        "email": r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}",
        "iban": r"[A-Z]{2}\d{2}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{2}",
        "phone": r"(?:\+?\d{1,3}[- ]?)?\(?\d{2,5}\)?[- ]?\d{3,10}",
        "name": r"\b[A-ZÀ-Ž][a-zà-ž]+\s+[A-ZÀ-Ž][a-zà-ž]+\b",
        "address": r"\b\d{5}\s+[A-ZÀ-Ž][a-zà-ž]+\b|\b[A-ZÀ-Ž][a-zà-žäöüß.-]+\s+\d+[a-z]?\b"
    }
    for p in patterns.values():
        if re.search(p, text): return True
    return False

# --- NEUE DYNAMISCHE FUNKTION FÜR AGENT-TOKEN-MAPPING ---
def assign_tokens_dynamically(events, target_keyword="Agent"):
    """
    Sammelt dynamisch Token-Verbräuche ein und ordnet sie dem zeitlich nächsten Node zu,
    der das target_keyword (z.B. 'Agent') im Namen trägt. Keine hardcodierten Namen nötig!
    """
    agent_events = [e for e in events if target_keyword.lower() in e['activity'].lower()]
    
    if not agent_events:
        return events 
        
    for event in events:
        if event['token_usage'] > 0 and event not in agent_events:
            best_agent = None
            min_time_diff = float('inf')
            event_start = datetime.strptime(event['start_timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            
            for agent in agent_events:
                agent_start = datetime.strptime(agent['start_timestamp'], '%Y-%m-%d %H:%M:%S.%f')
                diff = abs((event_start - agent_start).total_seconds())
                
                if diff < min_time_diff:
                    min_time_diff = diff
                    best_agent = agent
            
            if best_agent:
                best_agent['token_usage'] += event['token_usage']
                event['token_usage'] = 0
                
    return events

def extract_events(case_id, data_input):
    try:
        g = json.loads(data_input) if isinstance(data_input, str) else data_input
        root = get_pointer_val(g, 2)
        mapping = get_pointer_val(g, root.get('runData')) or {}
        events = []
        
        for activity, run_ptr in mapping.items():
            run_list = get_pointer_val(g, run_ptr) or []
            if not isinstance(run_list, list): run_list = [run_list]
                
            for meta_ptr in run_list:
                m = get_pointer_val(g, meta_ptr) or {}
                status = smart_resolve(g, m.get('executionStatus'))
                start_dt = datetime.fromtimestamp(m.get('startTime', 0) / 1000.0)
                dur_ms = m.get('executionTime', 0)
                
                # --- TOKEN FIX: Gemeinsames visited-Set verhindert Doppelzählung ---
                visited_for_node = set()
                token_total = extract_tokens_targeted(g, m.get('metadata'), visited_for_node)
                if token_total == 0:
                    token_total = extract_tokens_targeted(g, m.get('data'), visited_for_node)
                
                # --- DATENVOLUMEN & PII (Vollständige Pointersuche unverändert) ---
                resolved_payload = smart_resolve(g, m.get('data'))
                payload_json = json.dumps(resolved_payload, ensure_ascii=False)
                
                # Suchen nach fileSize im aufgelösten Baum
                file_size_match = re.search(r'"(?:fileSize|size)":\s*(\d+)', payload_json)
                volume = int(file_size_match.group(1)) if file_size_match else len(payload_json)
                
                # KI-Gewichtung
                if token_total > 0 and volume < 1000:
                    volume = max(volume, token_total * 4)

                events.append({
                    "case_id": case_id,
                    "activity": activity,
                    "start_timestamp": format_iso_timestamp(start_dt),
                    "end_timestamp": format_iso_timestamp(start_dt + timedelta(milliseconds=dur_ms)),
                    "execution_time_sec": dur_ms / 1000.0,
                    "token_usage": token_total,
                    "data_volume_bytes": volume,
                    "pii_detected": contains_pii(payload_json),
                    "execution_status": str(status),
                    "error_type": "None" if status == "success" else "Error"
                })
                
        # Dynamische Token-Zuweisung anwenden
        return assign_tokens_dynamically(events, target_keyword="Agent")
    except Exception as e:
        print(f" Fehler Case {case_id}: {e}")
        return []

def process_overhead(events):
    if not events: return []
    events.sort(key=lambda x: x['start_timestamp'])
    for i in range(1, len(events)):
        prev_end = datetime.strptime(events[i-1]['end_timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        curr_start = datetime.strptime(events[i]['start_timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        
        # TRICK: Mindestens 0.001 Sekunden (1 Millisekunde), damit IMMER Nachkommastellen exportiert werden
        overhead = max(0.001, (curr_start - prev_end).total_seconds())
        events[i]['system_overhead_sec'] = round(overhead, 3)
        
    if events: 
        # Auch das allererste Event bekommt 0.001 statt glatt 0.0
        events[0]['system_overhead_sec'] = 0.001
        
    return events

def run_pipeline():
    engine = get_db_engine()
    query = 'SELECT e.id, d.data FROM raw_execution_entity e JOIN raw_execution_data d ON e.id = d."executionId"'
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STARTE PRÄZISIONS-PIPELINE")
    first = True
    for chunk in pd.read_sql(query, engine, chunksize=100):
        all_ev = []
        for _, row in chunk.iterrows():
            all_ev.extend(process_overhead(extract_events(row['id'], row['data'])))
        if all_ev:
            df = pd.DataFrame(all_ev)
            # Zwingt Pandas und die Datenbank, diese Spalte als reinen Float zu behandeln
            df['system_overhead_sec'] = df['system_overhead_sec'].astype(float)
            df.to_sql('process_mining_events', engine, if_exists='replace' if first else 'append', index=False)
            first = False
    print(f"[{datetime.now().strftime('%H:%M:%S')}] PIPELINE FERTIG.")

if __name__ == "__main__":
    run_pipeline()