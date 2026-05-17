"""
Module 02: Process Transformation Pipeline
----------------------------------------
Transforms raw n8n API JSONs into a flat event-log structured format.
Implements Delta-Load architecture, PII detection, token usage extraction, 
Parent-Child mapping, and advanced Process Mining heuristics (DFG causality, loops, binary sizes).
"""

import os
import json
import re
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

load_dotenv()

def get_db_engine():
    """Establishes connection to the target PostgreSQL Data Warehouse."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    db = os.getenv("TARGET_DB_NAME")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_transformed_id(engine):
    """Retrieves the highest execution_id currently transformed to facilitate Delta Load."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(execution_id) FROM process_mining_events'))
            max_id = result.scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0

def format_iso_timestamp(dt):
    """Truncates Python datetime to milliseconds for Process Mining compatibility."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def get_exact_payload(node_data):
    """Extracts the actual operational JSON payload from the standard n8n node structure."""
    if not isinstance(node_data, dict): return {}
    output_key = next((k for k in node_data.keys() if k in ['main', 'ai_languageModel', 'ai_tool', 'ai_memory']), None)
    if not output_key: return {}
    try:
        return node_data[output_key][0][0].get('json', {})
    except (IndexError, KeyError, TypeError):
        return {}

def get_exact_total_tokens(node_data):
    """Parses node payload to extract LLM token consumption."""
    json_payload = get_exact_payload(node_data)
    if not json_payload: return 0
    
    if 'tokenUsageEstimate' in json_payload:
        usage = json_payload['tokenUsageEstimate']
        if isinstance(usage, dict): return int(usage.get('totalTokens', 0))
        
    if 'response' in json_payload:
        resp = json_payload['response']
        if isinstance(resp, dict) and 'response_metadata' in resp:
            meta = resp['response_metadata']
            if isinstance(meta, dict) and 'tokenUsage' in meta:
                usage = meta['tokenUsage']
                if isinstance(usage, dict): return int(usage.get('totalTokens', 0))
    return 0

def parse_file_size(size_str):
    """Parses human-readable file sizes (e.g. '15.3 kB') into exact bytes."""
    if not size_str: return 0
    size_str = str(size_str).strip().lower()
    match = re.search(r'([\d\.]+)\s*(kb|mb|gb|b|bytes)?', size_str)
    if not match: return 0
    
    val = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'kb': return int(val * 1024)
    if unit == 'mb': return int(val * 1024**2)
    if unit == 'gb': return int(val * 1024**3)
    return int(val)

def contains_pii(text):
    """Heuristic-based Personal Identifiable Information (PII) scanner."""
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

def process_overhead(events):
    """Calculates system/infrastructure latency between the nodes."""
    if not events: return []
    events.sort(key=lambda x: x['start_timestamp'])
    for i in range(1, len(events)):
        overhead = max(0.001, (events[i]['start_timestamp'] - events[i-1]['end_timestamp']).total_seconds())
        events[i]['system_overhead_sec'] = round(overhead, 3)
    if events: events[0]['system_overhead_sec'] = 0.001
    return events

def extract_events(execution_id, raw_api_data):
    """Parses a single n8n execution trace, resolving DFG edges, loops, and binary volumes."""
    try:
        # Safe parsing of execution payload
        if isinstance(raw_api_data, str):
            payload = json.loads(raw_api_data)
            if isinstance(payload, str): payload = json.loads(payload)
        else:
            payload = raw_api_data or {}

        if not isinstance(payload, dict):
            return []

        # Parent-Child mapping for sub-workflows
        parent_id = payload.get('parentExecution', {}).get('executionId')
        case_id = str(parent_id) if parent_id else str(execution_id)
        
        run_data = payload.get('resultData', {}).get('runData', {})
        if not run_data:
            return []
            
        events = []
        
        for activity, run_list in run_data.items():
            if not isinstance(run_list, list): run_list = [run_list]
            
            for loop_idx, m in enumerate(run_list):
                
                # Explicit Causality (Directly-Follows Graph Edge)
                sources = m.get('source', [])
                previous_node = sources[0].get('previousNode') if isinstance(sources, list) and sources else None

                status = str(m.get('executionStatus', 'unknown'))
                start_dt = datetime.fromtimestamp(m.get('startTime', 0) / 1000.0)
                
                # Zero-Millisecond Fix
                dur_ms = m.get('executionTime', 0)
                if dur_ms == 0: dur_ms = 1
                
                end_dt = start_dt + timedelta(milliseconds=dur_ms)
                
                node_data = m.get('data', {})
                token_total = get_exact_total_tokens(node_data)
                resolved_payload = get_exact_payload(node_data)
                payload_json = json.dumps(resolved_payload, ensure_ascii=False)
                
                # Exact Binary File Size Extraction
                binary_volume = 0
                try:
                    output_key = next((k for k in node_data.keys() if k in ['main', 'ai_languageModel', 'ai_tool', 'ai_memory']), None)
                    binary_dict = node_data[output_key][0][0].get('binary', {})
                    for key, val in binary_dict.items():
                        if isinstance(val, dict) and 'fileSize' in val:
                            binary_volume += parse_file_size(val.get('fileSize'))
                except Exception:
                    pass

                # Fallback to payload length if no binary data exists
                if binary_volume > 0:
                    volume = binary_volume
                else:
                    volume = len(payload_json)
                    if token_total > 0 and volume < 1000:
                        volume = max(volume, token_total * 4)

                events.append({
                    "execution_id": execution_id,
                    "case_id": case_id,
                    "activity": activity,
                    "previous_activity": previous_node,
                    "execution_index": loop_idx, 
                    "start_timestamp": start_dt,
                    "end_timestamp": end_dt,
                    "execution_time_sec": dur_ms / 1000.0,
                    "token_usage": token_total,
                    "data_volume_bytes": volume,
                    "pii_detected": contains_pii(payload_json),
                    "execution_status": status,
                    "error_type": "None" if status == "success" else "Error"
                })
        return events
    except Exception as e:
        logging.error(f"Failed to process execution {execution_id}: {e}")
        return []

def run_pipeline():
    """Main orchestration loop with Delta Load integration."""
    engine = get_db_engine()
    
    last_id = get_last_transformed_id(engine)
    logging.info(f"Initiating Transformation Pipeline. Delta load active (Fetching IDs > {last_id}).")
    
    query = f'SELECT id, data FROM raw_api_executions WHERE CAST(id AS INTEGER) > {last_id} ORDER BY CAST(id AS INTEGER) ASC'
    total_new_events = 0
    
    for chunk in pd.read_sql(query, engine, chunksize=100):
        all_ev = []
        for _, row in chunk.iterrows():
            raw_data = row.get('data', {})
            case_events = extract_events(row['id'], raw_data)
            
            if case_events:
                case_events = process_overhead(case_events)
                
                for ev in case_events:
                    ev['start_timestamp'] = format_iso_timestamp(ev['start_timestamp'])
                    ev['end_timestamp'] = format_iso_timestamp(ev['end_timestamp'])
                
                all_ev.extend(case_events)
            
        if all_ev:
            df = pd.DataFrame(all_ev)
            df['system_overhead_sec'] = df['system_overhead_sec'].astype(float)
            df.to_sql('process_mining_events', engine, if_exists='append', index=False)
            total_new_events += len(df)
            
    logging.info(f"Pipeline successfully completed. New process events appended: {total_new_events}")

if __name__ == "__main__":
    run_pipeline()