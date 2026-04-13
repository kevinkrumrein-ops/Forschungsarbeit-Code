import os, json, re, pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_db_engine():
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    db = os.getenv("TARGET_DB_NAME")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def format_iso_timestamp(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def get_pointer_val(g, ptr):
    try:
        if isinstance(ptr, str) and ptr.isdigit():
            idx = int(ptr)
            if isinstance(g, list): return g[idx] if 0 <= idx < len(g) else None
        return None
    except: return None

def resolve_pure_payload(g, obj, visited=None, depth=0):
    if visited is None: visited = set()
    if depth > 20: return "[Max Depth]" 
    if isinstance(obj, str) and obj.isdigit():
        if obj in visited: return obj 
        visited.add(obj)
        resolved = get_pointer_val(g, obj)
        if resolved is not None:
            return resolve_pure_payload(g, resolved, visited, depth + 1)
        return obj
    if isinstance(obj, dict):
        return {k: resolve_pure_payload(g, v, visited.copy(), depth + 1) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_pure_payload(g, v, visited.copy(), depth + 1) for v in obj]
    return obj

def get_exact_payload(g, data_ptr):
    data_obj = get_pointer_val(g, data_ptr)
    if not isinstance(data_obj, dict): return {}
    output_key = next((k for k in data_obj.keys() if k in ['main', 'ai_languageModel', 'ai_tool', 'ai_memory']), None)
    if not output_key: return {}
    output_branches = get_pointer_val(g, data_obj[output_key])
    if not isinstance(output_branches, list) or len(output_branches) == 0: return {}
    branch_items = get_pointer_val(g, output_branches[0])
    if not isinstance(branch_items, list) or len(branch_items) == 0: return {}
    first_item = get_pointer_val(g, branch_items[0])
    if not isinstance(first_item, dict) or 'json' not in first_item: return {}
    json_payload = get_pointer_val(g, first_item['json'])
    if not isinstance(json_payload, dict): return {}
    return resolve_pure_payload(g, json_payload)

def get_exact_total_tokens(g, data_ptr):
    data_obj = get_pointer_val(g, data_ptr)
    if not isinstance(data_obj, dict): return 0
    output_key = 'ai_languageModel' if 'ai_languageModel' in data_obj else 'main'
    if output_key not in data_obj: return 0
    output_branches = get_pointer_val(g, data_obj[output_key])
    if not isinstance(output_branches, list) or len(output_branches) == 0: return 0
    branch_items = get_pointer_val(g, output_branches[0])
    if not isinstance(branch_items, list) or len(branch_items) == 0: return 0
    first_item = get_pointer_val(g, branch_items[0])
    if not isinstance(first_item, dict) or 'json' not in first_item: return 0
    json_payload = get_pointer_val(g, first_item['json'])
    if not isinstance(json_payload, dict): return 0
    
    token_usage = None
    if 'tokenUsageEstimate' in json_payload:
        token_usage = get_pointer_val(g, json_payload['tokenUsageEstimate'])
    elif 'response' in json_payload:
        response_obj = get_pointer_val(g, json_payload['response'])
        if isinstance(response_obj, dict) and 'response_metadata' in response_obj:
            resp_meta = get_pointer_val(g, response_obj['response_metadata'])
            if isinstance(resp_meta, dict) and 'tokenUsage' in resp_meta:
                token_usage = resp_meta['tokenUsage']
                
    if not token_usage: return 0
    if isinstance(token_usage, str) and token_usage.isdigit():
        token_usage = get_pointer_val(g, token_usage)
    if isinstance(token_usage, dict) and 'totalTokens' in token_usage:
        return int(token_usage['totalTokens'])
    return 0

def contains_pii(text):
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

def resolve_simultaneity(events):
    """
    Leverage microsecond precision for causal ordering before truncating 
    to millisecond resolution for process mining compatibility.
    """
    if not events: return []
    
    # Sort by full microsecond precision to establish clear causal order
    events.sort(key=lambda x: x['start_timestamp'])
    
    for i in range(1, len(events)):
        prev_event = events[i-1]
        curr_event = events[i]
        
        # Check if current start millisecond matches previous end millisecond
        prev_end_ms = format_iso_timestamp(prev_event['end_timestamp'])
        curr_start_ms = format_iso_timestamp(curr_event['start_timestamp'])
        
        if prev_end_ms == curr_start_ms:
            # Add 1ms to ensure visibility of the causal link after truncation
            curr_event['start_timestamp'] += timedelta(milliseconds=1)
            curr_event['end_timestamp'] += timedelta(milliseconds=1)
            
    return events

def extract_events(case_id, data_input):
    try:
        g = json.loads(data_input) if isinstance(data_input, str) else data_input
        root = get_pointer_val(g, "2")
        mapping = get_pointer_val(g, root.get('runData')) or {}
        events = []
        
        for activity, run_ptr in mapping.items():
            run_list = get_pointer_val(g, run_ptr) or []
            if not isinstance(run_list, list): run_list = [run_list]
            for meta_ptr in run_list:
                m = get_pointer_val(g, meta_ptr) or {}
                status_ptr = m.get('executionStatus')
                status = get_pointer_val(g, status_ptr) if (isinstance(status_ptr, str) and status_ptr.isdigit()) else status_ptr
                
                # Maintain full datetime objects for causal ordering
                start_dt = datetime.fromtimestamp(m.get('startTime', 0) / 1000.0)
                dur_ms = m.get('executionTime', 0)
                end_dt = start_dt + timedelta(milliseconds=dur_ms)
                
                token_total = get_exact_total_tokens(g, m.get('data'))
                resolved_payload = get_exact_payload(g, m.get('data'))
                payload_json = json.dumps(resolved_payload, ensure_ascii=False)
                file_size_match = re.search(r'"(?:fileSize|size)":\s*(\d+)', payload_json)
                volume = int(file_size_match.group(1)) if file_size_match else len(payload_json)
                
                if token_total > 0 and volume < 1000:
                    volume = max(volume, token_total * 4)

                events.append({
                    "case_id": case_id,
                    "activity": activity,
                    "start_timestamp": start_dt,
                    "end_timestamp": end_dt,
                    "execution_time_sec": dur_ms / 1000.0,
                    "token_usage": token_total,
                    "data_volume_bytes": volume,
                    "pii_detected": contains_pii(payload_json),
                    "execution_status": str(status),
                    "error_type": "None" if status == "success" else "Error"
                })
        return events
    except Exception as e:
        print(f" Fehler Case {case_id}: {e}")
        return []

def process_overhead(events):
    """Calculates system overhead between events using datetime objects."""
    if not events: return []
    # Already sorted by resolve_simultaneity
    for i in range(1, len(events)):
        prev_end = events[i-1]['end_timestamp']
        curr_start = events[i]['start_timestamp']
        overhead = max(0.001, (curr_start - prev_end).total_seconds())
        events[i]['system_overhead_sec'] = round(overhead, 3)
    if events: events[0]['system_overhead_sec'] = 0.001
    return events

def run_pipeline():
    engine = get_db_engine()
    query = 'SELECT e.id, d.data FROM raw_execution_entity e JOIN raw_execution_data d ON e.id = d."executionId"'
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STARTE KAUSAL-PIPELINE (Mikrosekunden-Präzision)")
    first = True
    for chunk in pd.read_sql(query, engine, chunksize=100):
        all_ev = []
        for _, row in chunk.iterrows():
            # 1. Extract raw datetimes
            case_events = extract_events(row['id'], row['data'])
            # 2. Resolve simultaneity based on microsecond causal order
            case_events = resolve_simultaneity(case_events)
            # 3. Process overhead
            case_events = process_overhead(case_events)
            
            # 4. Final Formatting: Convert to ISO strings (ms resolution) before storage
            for ev in case_events:
                ev['start_timestamp'] = format_iso_timestamp(ev['start_timestamp'])
                ev['end_timestamp'] = format_iso_timestamp(ev['end_timestamp'])
            
            all_ev.extend(case_events)
            
        if all_ev:
            df = pd.DataFrame(all_ev)
            df['system_overhead_sec'] = df['system_overhead_sec'].astype(float)
            df.to_sql('process_mining_events', engine, if_exists='replace' if first else 'append', index=False)
            first = False
    print(f"[{datetime.now().strftime('%H:%M:%S')}] PIPELINE FERTIG.")

if __name__ == "__main__":
    run_pipeline()
