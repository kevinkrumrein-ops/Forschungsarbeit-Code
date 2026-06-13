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

# ---------------------------------------------------------------------------
# Generic, framework-independent LLM token accounting.
#
# Instead of hard-coding a single JSON path (which breaks the moment a new LLM
# family stores its counters under a different key), we recognize the small,
# stable set of *key-name synonyms* that virtually every provider/framework
# uses (OpenAI, LangChain chains, Anthropic, local models via LangChain, ...).
# Key names are normalized to lowercase without underscores before matching.
# ---------------------------------------------------------------------------
TOKEN_TOTAL_KEYS = {"totaltokens"}                      # totalTokens, total_tokens
TOKEN_PROMPT_KEYS = {"prompttokens", "inputtokens"}     # promptTokens, input_tokens, prompt_tokens
TOKEN_COMPLETION_KEYS = {"completiontokens", "outputtokens"}  # completionTokens, output_tokens

def _to_int(value) -> int:
    """Safely coerces a token counter (int/float/numeric string) into an int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

def find_token_usage(obj) -> int:
    """
    Recursively searches any nested structure for LLM token counters, independent
    of the concrete JSON path or LLM framework. Matches a fixed set of synonym key
    names rather than a fixed location. Prefers an explicit total; otherwise falls
    back to prompt + completion. Uses max() across levels to avoid double counting
    when the same usage block is reported at several nesting depths.
    """
    if isinstance(obj, dict):
        total = 0
        prompt = 0
        completion = 0
        for key, value in obj.items():
            norm = str(key).lower().replace("_", "")
            if norm in TOKEN_TOTAL_KEYS and isinstance(value, (int, float, str)):
                total = max(total, _to_int(value))
            elif norm in TOKEN_PROMPT_KEYS and isinstance(value, (int, float, str)):
                prompt = max(prompt, _to_int(value))
            elif norm in TOKEN_COMPLETION_KEYS and isinstance(value, (int, float, str)):
                completion = max(completion, _to_int(value))
            else:
                nested = find_token_usage(value)
                if nested > 0:
                    total = max(total, nested)
        if total > 0:
            return total
        if prompt or completion:
            return prompt + completion
        return 0
    if isinstance(obj, list):
        best = 0
        for item in obj:
            best = max(best, find_token_usage(item))
        return best
    return 0

def extract_node_metrics(node_data: dict) -> tuple:
    """
    Unified extraction pass. Traverses the complex nested n8n JSON payload exactly once
    to extract the functional payload, LLM tokens, and binary attachment sizes.
    """
    if not isinstance(node_data, dict):
        return {}, 0, 0

    # 1. Generic, path-independent token search across the entire node payload.
    #    Works for any LLM family because it keys off synonym names, not a fixed path.
    tokens = find_token_usage(node_data)

    # Standard output channels where n8n registers node run executions
    target_keys = ['main', 'ai_languageModel', 'ai_tool', 'ai_memory']
    output_key = next((k for k in node_data.keys() if k in target_keys), None)
    if not output_key:
        return {}, tokens, 0

    try:
        execution_bucket = node_data[output_key][0][0]
        json_payload = execution_bucket.get('json', {})

        # 2. Compute cumulative binary file volumes
        binary_bytes = 0
        binary_dict = execution_bucket.get('binary', {})
        if isinstance(binary_dict, dict):
            for file_meta in binary_dict.values():
                if isinstance(file_meta, dict) and 'fileSize' in file_meta:
                    binary_bytes += parse_file_size(file_meta.get('fileSize'))

        return json_payload, tokens, binary_bytes
    except (IndexError, KeyError, TypeError):
        return {}, tokens, 0

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

def _extract_parent_id(raw_api_data):
    """Returns the immediate parent execution id of a raw n8n trace, or None."""
    try:
        if isinstance(raw_api_data, str):
            payload = json.loads(raw_api_data)
            if isinstance(payload, str):
                payload = json.loads(payload)
        else:
            payload = raw_api_data or {}
        if isinstance(payload, dict):
            parent = payload.get('parentExecution') or {}
            if isinstance(parent, dict):
                pid = parent.get('executionId')
                return str(pid) if pid is not None else None
    except Exception:
        return None
    return None

def build_lineage_maps(engine) -> tuple:
    """
    In a single pass over the ENTIRE warehouse (not just the current delta), builds:
      - parent_map      : {execution_id -> immediate_parent_execution_id}
      - workflow_id_map : {execution_id -> workflow_id}

    The parent map is required to resolve multi-level sub-workflow / self-call chains
    back to their root execution, independent of recursion depth. The workflow_id map
    lets us tag every event with the *root* workflow (the workflow in which the case
    actually started), so a case that spans a main workflow plus its tool sub-workflows
    is never torn apart when exporting per workflow.
    """
    parent_map = {}
    workflow_id_map = {}
    try:
        for chunk in pd.read_sql('SELECT id, data, "workflowId" FROM raw_api_executions', engine, chunksize=200):
            for _, row in chunk.iterrows():
                exec_id = str(row['id'])
                wf_id = row.get('workflowId')
                if wf_id is not None:
                    workflow_id_map[exec_id] = str(wf_id)
                parent_id = _extract_parent_id(row.get('data'))
                if parent_id:
                    parent_map[exec_id] = parent_id
    except Exception as e:
        logger.warning(f"[TRANSFORMER] Could not build lineage maps (falling back to single-level stitching): {str(e)}")
    return parent_map, workflow_id_map

def resolve_root_case(execution_id, parent_map: dict) -> str:
    """
    Walks the parent chain upwards until the root execution (no further parent) is
    reached, so every event of one logical run shares a single case_id regardless of
    recursion depth. Includes a cycle guard to stay safe on malformed graphs.
    """
    current = str(execution_id)
    seen = set()
    while current in parent_map and current not in seen:
        seen.add(current)
        current = parent_map[current]
    return current

def extract_events(execution_id, raw_api_data, workflow_id=None, root_case_id=None, root_workflow_id=None) -> list:
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

        # Maintain graph relationship across parent workflows and asynchronous sub-workflows.
        # Prefer the pre-resolved ROOT case (collapses multi-level self-call chains into one
        # case); fall back to single-level parent stitching if no map was provided.
        if root_case_id is not None:
            case_id = str(root_case_id)
        else:
            parent_id = payload.get('parentExecution', {}).get('executionId')
            case_id = str(parent_id) if parent_id else str(execution_id)

        # Workflow provenance: keep each workflow distinguishable so downstream event
        # logs are never mixed across different workflows during Process Mining.
        # 'workflow_id'      = the workflow that executed THIS node (may be a tool sub-workflow)
        # 'root_workflow_id' = the workflow in which the whole case started (stable grouping key)
        workflow_id = str(workflow_id) if workflow_id is not None else None
        root_workflow_id = str(root_workflow_id) if root_workflow_id is not None else workflow_id
        
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
                    "workflow_id": workflow_id,
                    "root_workflow_id": root_workflow_id,
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

    # Build the full execution->parent and execution->workflow maps once, so sub-workflow
    # / self-call chains can be resolved to their root case AND tagged with the root workflow,
    # even across delta loads.
    parent_map, workflow_id_map = build_lineage_maps(engine)

    query = f'SELECT id, data, "workflowId" FROM raw_api_executions WHERE CAST(id AS INTEGER) > {last_id} ORDER BY CAST(id AS INTEGER) ASC'
    total_new_events = 0
    
    try:
        for chunk in pd.read_sql(query, engine, chunksize=100):
            all_events = []
            for _, row in chunk.iterrows():
                exec_id = str(row['id'])
                root_case = resolve_root_case(exec_id, parent_map)

                # Determine the ROOT workflow without mislabeling broken chains:
                #  - root known in the warehouse -> use the root's workflow
                #  - root is this execution itself -> genuine top-level run, use own workflow
                #  - root points to a parent that is MISSING (referential gap) -> mark
                #    'unresolved' instead of falsely attributing a sub-run (e.g. a tool)
                #    to its own workflow id.
                if root_case in workflow_id_map:
                    root_wf = workflow_id_map[root_case]
                elif root_case == exec_id:
                    root_wf = row.get('workflowId')
                else:
                    root_wf = "unresolved"

                case_events = extract_events(row['id'], row.get('data', {}), row.get('workflowId'), root_case, root_wf)
                
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
