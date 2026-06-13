"""
Module 02a: Event Log Transformer
---------------------------------
First half of the transformation stage: turns the raw n8n API JSON into a flat,
workflow-agnostic event log. Its sibling 02b_transform_case_log builds the case
table from this output.

Covers incremental delta-load, PII detection, framework-independent token
accounting, and per-case system-overhead timing for process mining.
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

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TRANSFORMER")

load_dotenv()

EVENTS_TABLE = "process_mining_events"

# Token-counter key synonyms, normalized to lowercase without underscores. Matching by key name rather than a fixed JSON path keeps token extraction working across LLM providers/frameworks.
TOKEN_TOTAL_KEYS = {"totaltokens"}
TOKEN_PROMPT_KEYS = {"prompttokens", "inputtokens"}
TOKEN_COMPLETION_KEYS = {"completiontokens", "outputtokens"}

def get_target_engine():
    """Create the SQLAlchemy engine for the target PostgreSQL warehouse."""
    user = os.getenv("TARGET_DB_USER")
    pw = os.getenv("TARGET_DB_PASSWORD")
    host = os.getenv("TARGET_DB_HOST", "127.0.0.1")
    port = os.getenv("TARGET_DB_PORT", "5434")
    db = os.getenv("TARGET_DB_NAME")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

def get_last_transformed_id(engine) -> int:
    """Return the highest execution ID already transformed, or 0 if the table is missing."""
    try:
        with engine.connect() as conn:
            max_id = conn.execute(text(f"SELECT MAX(execution_id) FROM {EVENTS_TABLE}")).scalar()
            return int(max_id) if max_id is not None else 0
    except Exception:
        return 0

def parse_file_size(size_str) -> int:
    """Convert a human-readable n8n file size (e.g. '15.3 kB') into bytes."""
    if not size_str:
        return 0
    size_str = str(size_str).strip().lower()
    match = re.search(r"([\d\.]+)\s*(kb|mb|gb|b|bytes)?", size_str)
    if not match:
        return 0
    val = float(match.group(1))
    unit = match.group(2)
    if unit == "kb":
        return int(val * 1024)
    if unit == "mb":
        return int(val * 1024 ** 2)
    if unit == "gb":
        return int(val * 1024 ** 3)
    return int(val)

def contains_pii(text_content) -> bool:
    """Scan text for common PII patterns (email, IBAN, phone, full name)."""
    patterns = {
        "email": r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}",
        "iban": r"[A-Z]{2}\d{2}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{4}[ ]?\d{2}",
        "phone": r"(?:\+?\d{1,3}[- ]?)?\(?\d{2,5}\)?[- ]?\d{3,10}",
        "name": r"\b[A-ZÀ-Ž][a-zà-ž]+\s+[A-ZÀ-Ž][a-zà-ž]+\b",
    }
    return any(re.search(p, text_content) for p in patterns.values())

def _to_int(value) -> int:
    """Coerce a token counter (int/float/numeric string) into an int, defaulting to 0."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

def find_token_usage(obj) -> int:
    """
    Recursively search a nested structure for token counters by key name. Prefers an
    explicit total; otherwise sums prompt + completion. Uses max() across nesting
    levels to avoid double counting when usage is reported at several depths.
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
        return max((find_token_usage(item) for item in obj), default=0)
    return 0

def extract_node_metrics(node_data: dict) -> tuple:
    """
    Traverse a node's payload once to extract its functional JSON, total token usage,
    and cumulative binary attachment size in bytes.
    """
    if not isinstance(node_data, dict):
        return {}, 0, 0

    tokens = find_token_usage(node_data)

    # n8n registers node run output under one of these channels.
    target_keys = ["main", "ai_languageModel", "ai_tool", "ai_memory"]
    output_key = next((k for k in node_data.keys() if k in target_keys), None)
    if not output_key:
        return {}, tokens, 0

    try:
        execution_bucket = node_data[output_key][0][0]
        json_payload = execution_bucket.get("json", {})

        binary_bytes = 0
        binary_dict = execution_bucket.get("binary", {})
        if isinstance(binary_dict, dict):
            for file_meta in binary_dict.values():
                if isinstance(file_meta, dict) and "fileSize" in file_meta:
                    binary_bytes += parse_file_size(file_meta.get("fileSize"))

        return json_payload, tokens, binary_bytes
    except (IndexError, KeyError, TypeError):
        return {}, tokens, 0

def calculate_system_overhead(events: list) -> list:
    """Add the infrastructure latency (gap to the previous node) to each event."""
    if not events:
        return []
    events.sort(key=lambda x: x["start_timestamp"])
    for i in range(1, len(events)):
        overhead = (events[i]["start_timestamp"] - events[i - 1]["end_timestamp"]).total_seconds()
        events[i]["system_overhead_sec"] = round(max(0.001, overhead), 3)
    events[0]["system_overhead_sec"] = 0.001
    return events

def _extract_parent_id(raw_api_data):
    """Return the immediate parent execution id of a raw n8n trace, or None."""
    try:
        if isinstance(raw_api_data, str):
            payload = json.loads(raw_api_data)
            if isinstance(payload, str):
                payload = json.loads(payload)
        else:
            payload = raw_api_data or {}
        if isinstance(payload, dict):
            parent = payload.get("parentExecution") or {}
            if isinstance(parent, dict):
                pid = parent.get("executionId")
                return str(pid) if pid is not None else None
    except Exception:
        return None
    return None

def build_lineage_maps(engine) -> tuple:
    """
    Build, in one pass over the whole warehouse, two maps:
      parent_map      : execution_id -> immediate parent execution_id
      workflow_id_map : execution_id -> workflow_id

    The parent map resolves multi-level sub-workflow / self-call chains back to their
    root execution; the workflow map lets every event be tagged with the root workflow,
    so a case spanning a main workflow and its tool sub-workflows stays in one file.
    """
    parent_map = {}
    workflow_id_map = {}
    try:
        for chunk in pd.read_sql('SELECT id, data, "workflowId" FROM raw_api_executions', engine, chunksize=200):
            for _, row in chunk.iterrows():
                exec_id = str(row["id"])
                wf_id = row.get("workflowId")
                if wf_id is not None:
                    workflow_id_map[exec_id] = str(wf_id)
                parent_id = _extract_parent_id(row.get("data"))
                if parent_id:
                    parent_map[exec_id] = parent_id
    except Exception as e:
        logger.warning(f"Could not build lineage maps, falling back to single-level stitching: {e}")
    return parent_map, workflow_id_map

def resolve_root_case(execution_id, parent_map: dict) -> str:
    """Walk the parent chain up to the root execution. Cycle-guarded for malformed graphs."""
    current = str(execution_id)
    seen = set()
    while current in parent_map and current not in seen:
        seen.add(current)
        current = parent_map[current]
    return current

def extract_events(execution_id, raw_api_data, workflow_id=None, root_case_id=None, root_workflow_id=None) -> list:
    """Parse one raw n8n execution trace into standardized event-log entries."""
    try:
        if isinstance(raw_api_data, str):
            payload = json.loads(raw_api_data)
            if isinstance(payload, str):
                payload = json.loads(payload)
        else:
            payload = raw_api_data or {}

        if not isinstance(payload, dict):
            return []

        # Prefer the pre-resolved root case (collapses multi-level self-calls into one case); fall back to single-level parent stitching if no map was provided.
        if root_case_id is not None:
            case_id = str(root_case_id)
        else:
            parent_id = payload.get("parentExecution", {}).get("executionId")
            case_id = str(parent_id) if parent_id else str(execution_id)

        # workflow_id      = workflow that ran THIS node (may be a tool sub-workflow)
        # root_workflow_id = workflow the case started in (stable grouping key)
        workflow_id = str(workflow_id) if workflow_id is not None else None
        root_workflow_id = str(root_workflow_id) if root_workflow_id is not None else workflow_id

        run_data = payload.get("resultData", {}).get("runData", {})
        if not run_data:
            return []

        events = []
        for activity, run_list in run_data.items():
            if not isinstance(run_list, list):
                run_list = [run_list]

            for loop_idx, meta in enumerate(run_list):
                sources = meta.get("source", [])
                previous_node = sources[0].get("previousNode") if isinstance(sources, list) and sources else None
                status = str(meta.get("executionStatus", "unknown"))

                start_dt = datetime.fromtimestamp(meta.get("startTime", 0) / 1000.0)
                # Floor duration at 1ms so zero-duration nodes keep a stable Celonis sort order.
                duration_ms = meta.get("executionTime", 0)
                end_dt = start_dt + timedelta(milliseconds=max(1, duration_ms))

                resolved_payload, token_total, binary_volume = extract_node_metrics(meta.get("data", {}))
                payload_json = json.dumps(resolved_payload, ensure_ascii=False)

                # Use binary size as the data footprint; otherwise the JSON length, scaled up by token count for small LLM payloads that carry little raw JSON.
                if binary_volume > 0:
                    volume = binary_volume
                else:
                    volume = len(payload_json)
                    if token_total > 0 and volume < 1000:
                        volume = max(volume, token_total * 4)

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
                    "error_type": "None" if status == "success" else "Error",
                })
        return events
    except Exception as e:
        logger.error(f"Failed to process execution trace {execution_id}: {e}")
        return []

def run_transformation_pipeline():
    """Transform new raw executions into events and append them to the warehouse."""
    engine = get_target_engine()
    last_id = get_last_transformed_id(engine)
    logger.info(f"Processing new records since execution ID {last_id}")

    # Built once over the whole warehouse so sub-workflow / self-call chains resolve to their root case and workflow even across delta loads.
    parent_map, workflow_id_map = build_lineage_maps(engine)

    query = (f'SELECT id, data, "workflowId" FROM raw_api_executions '
             f"WHERE CAST(id AS INTEGER) > {last_id} ORDER BY CAST(id AS INTEGER) ASC")
    total_new_events = 0

    try:
        for chunk in pd.read_sql(query, engine, chunksize=100):
            all_events = []
            for _, row in chunk.iterrows():
                exec_id = str(row["id"])
                root_case = resolve_root_case(exec_id, parent_map)

                # Determine the root workflow:
                #   root known         -> use the root's workflow
                #   root is this run   -> genuine top-level run, use own workflow
                #   root parent missing -> referential gap, mark 'unresolved' instead of
                #                          mislabeling a sub-run with its own workflow id
                if root_case in workflow_id_map:
                    root_wf = workflow_id_map[root_case]
                elif root_case == exec_id:
                    root_wf = row.get("workflowId")
                else:
                    root_wf = "unresolved"

                case_events = extract_events(row["id"], row.get("data", {}), row.get("workflowId"), root_case, root_wf)
                if case_events:
                    case_events = calculate_system_overhead(case_events)
                    all_events.extend(case_events)

            if all_events:
                df = pd.DataFrame(all_events)
                df["system_overhead_sec"] = df["system_overhead_sec"].astype(float)
                df.to_sql(EVENTS_TABLE, engine, if_exists="append", index=False)
                total_new_events += len(df)
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)

    logger.info(f"Transformation finished, appended {total_new_events} new process events")

if __name__ == "__main__":
    run_transformation_pipeline()