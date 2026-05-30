"""
Module 02: Process Transformation Pipeline
------------------------------------------
Transforms raw n8n API JSON data into a clean, flat event-log structure.
Implements Delta-Load extraction, PII compliance checks, token metrics auditing, 
and calculates infrastructure latencies for advanced Process Mining.
"""

import os
import sys
import re
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Uniform logging configuration matching the pipeline architecture
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("TRANSFORMER")

load_dotenv()

def get_target_engine():
    """Initializes and returns the SQLAlchemy connection engine for PostgreSQL."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    db = os.getenv("TARGET_DB_NAME")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_transformed_id(engine) -> int:
    """Retrieves the highest processed execution ID to guarantee a consistent Delta Load."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(execution_id) FROM process_mining_events'))
            max_id = result.scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0  # Fallback if the target table has not been provisioned yet
        
def parse_file_size(size_str) -> int:
    """Converts human-readable n8n file sizes (e.g., '15.3 kB') into exact bytes."""
    if not size_str:
        return 0
    size_str = str(size_str).strip().lower()
    match = re.search(r'([\d\.]+)\s*(kb|mb|gb|b|bytes)?', size_str)
    if not match:
        return 0
    
    val = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'kb': return int(val * 1024)
    if unit == 'mb': return int(val * 1024 ** 2)
    if unit == 'gb': return int(val * 1024 ** 3)
    return int(val)

def contains_pii(text_content) -> bool:
    """Scans structured text data for common Personally Identifiable Information (PII) patterns."""
    patterns = {
        "email": r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}",
        "iban": r"[A-Z]{2}\d{2}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{2}",
        "phone": r"(?:\+?\d{1,3}[- ]?)?\(?\d{2,5}\)?[- ]?\d{3,10}",
        "name": r"\b[A-ZÀ-Ž][a-zà-ž]+\s+[A-ZÀ-Ž][a-zà-ž]+\b"
    }
    return any(re.search(p, text_content) for p in patterns.values())

def extract_node_metrics(node_data: dict) -> tuple:
    """
    Unified extraction pass. Traverses the complex nested n8n JSON payload exactly once
    to extract the functional payload, LLM tokens, and binary attachment sizes.
    """
    if not isinstance(node_data, dict):
        return {}, 0, 0
        
    # Standard output channels where n8n registers node run executions
    target_keys = ['main', 'ai_languageModel', 'ai_tool', 'ai_memory']
    output_key = next((k for k in node_data.keys() if k in target_keys), None)
    if not output_key:
        return {}, 0, 0
        
    try:
        execution_bucket = node_data[output_key][0][0]
        json_payload = execution_bucket.get('json', {})
        
        # 1. Parse Token Consumptions from varying LLM framework response structures
        tokens = 0
        if 'tokenUsageEstimate' in json_payload:
            usage = json_payload['tokenUsageEstimate']
            if isinstance(usage, dict): 
                tokens = int(usage.get('totalTokens', 0))
        elif 'response' in json_payload:
            resp = json_payload['response']
            if isinstance(resp, dict) and 'response_metadata' in resp:
                meta = resp['response_metadata']
                if isinstance(meta, dict) and 'tokenUsage' in meta:
                    usage = meta['tokenUsage']
                    if isinstance(usage, dict): 
                        tokens = int(usage.get('totalTokens', 0))
                        
        # 2. Compute cumulative binary file volumes
        binary_bytes = 0
        binary_dict = execution_bucket.get('binary', {})
        if isinstance(binary_dict, dict):
            for file_meta in binary_dict.values():
                if isinstance(file_meta, dict) and 'fileSize' in file_meta:
                    binary_bytes += parse_file_size(file_meta.get('fileSize'))
                    
        return json_payload, tokens, binary_bytes
    except (IndexError, KeyError, TypeError):
        return {}, 0, 0

def calculate_system_overhead(events: list) -> list:
    """Calculates chronological infrastructure latency between consecutive node executions."""
    if not events:
        return []
    events.sort(key=lambda x: x['start_timestamp'])
    for i in range(1, len(events)):
        overhead = (events[i]['start_timestamp'] - events[i-1]['end_timestamp']).total_seconds()
        events[i]['system_overhead_sec'] = round(max(0.001, overhead), 3)
    events[0]['system_overhead_sec'] = 0.001
    return events

def extract_events(execution_id, raw_api_data) -> list:
    """Parses a standalone raw n8n execution trace into standardized process log entries."""
    try:
        if isinstance(raw_api_data, str):
            payload = json.loads(raw_api_data)
            if isinstance(payload, str): 
                payload = json.loads(payload)
        else:
            payload = raw_api_data or {}

        if not isinstance(payload, dict):
            return []

        # Maintain graph relationship across parent workflows and asynchronous sub-workflows
        parent_id = payload.get('parentExecution', {}).get('executionId')
        case_id = str(parent_id) if parent_id else str(execution_id)
        
        run_data = payload.get('resultData', {}).get('runData', {})
        if not run_data:
            return []
            
        events = []
        for activity, run_list in run_data.items():
            if not isinstance(run_list, list): 
                run_list = [run_list]
                
            for loop_idx, meta in enumerate(run_list):
                sources = meta.get('source', [])
                previous_node = sources[0].get('previousNode') if isinstance(sources, list) and sources else None
                status = str(meta.get('executionStatus', 'unknown'))
                
                start_dt = datetime.fromtimestamp(meta.get('startTime', 0) / 1000.0)
                
                # Prevent mathematical duration collapses (0ms) to preserve Celonis sorting integrity
                duration_ms = meta.get('executionTime', 0)
                end_dt = start_dt + timedelta(milliseconds=max(1, duration_ms))
                
                # High-performance metric extraction pass
                resolved_payload, token_total, binary_volume = extract_node_metrics(meta.get('data', {}))
                payload_json = json.dumps(resolved_payload, ensure_ascii=False)
                
                # Assign data footprint using binary sizes, falling back to string block length
                if binary_volume > 0:
                    volume = binary_volume
                else:
                    volume = len(payload_json)
                    if token_total > 0 and volume < 1000:
                        volume = max(volume, token_total * 4) # Fallback scaling for LLM parameters

                events.append({
                    "execution_id": execution_id,
                    "case_id": case_id,
                    "activity": activity,
                    "previous_activity": previous_node,
                    "execution_index": loop_idx, 
                    "start_timestamp": start_dt,
                    "end_timestamp": end_dt,
                    "execution_time_sec": max(0.001, duration_ms / 1000.0),
                    "token_usage": token_total,
                    "data_volume_bytes": volume,
                    "pii_detected": contains_pii(payload_json),
                    "execution_status": status,
                    "error_type": "None" if status == "success" else "Error"
                })
        return events
    except Exception as e:
        logger.error(f"[TRANSFORMER] Failed to process execution trace {execution_id}: {str(e)}")
        return []

def run_transformation_pipeline():
    """Executes the delta data transformation stage and materializes results into the warehouse."""
    engine = get_target_engine()
    last_id = get_last_transformed_id(engine)
    
    # LINE 1: Clean startup logging
    logger.info(f"[TRANSFORMER] Processing new records since execution ID {last_id}.")
    
    query = f'SELECT id, data FROM raw_api_executions WHERE CAST(id AS INTEGER) > {last_id} ORDER BY CAST(id AS INTEGER) ASC'
    total_new_events = 0
    
    try:
        for chunk in pd.read_sql(query, engine, chunksize=100):
            all_events = []
            for _, row in chunk.iterrows():
                case_events = extract_events(row['id'], row.get('data', {}))
                
                if case_events:
                    # Injects network/engine delay variables across the sequential execution graph
                    case_events = calculate_system_overhead(case_events)
                    all_events.extend(case_events)
                
            if all_events:
                df = pd.DataFrame(all_events)
                df['system_overhead_sec'] = df['system_overhead_sec'].astype(float)
                df.to_sql('process_mining_events', engine, if_exists='append', index=False)
                total_new_events += len(df)
    except Exception as e:
        logger.error(f"[TRANSFORMER] Critical pipeline failure: {str(e)}")
        sys.exit(1)
            
    # LINE 2: Clean outcome logging
    logger.info(f"[TRANSFORMER] Transformation finished. Appended {total_new_events} new process events to database.")

if __name__ == "__main__":
    run_transformation_pipeline()