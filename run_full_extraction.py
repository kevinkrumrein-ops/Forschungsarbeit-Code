"""
Module 00: ETL Pipeline Orchestrator
------------------------------------
Sequentially orchestrates the execution of the n8n Process Mining ETL pipeline.

The core pipeline (extract -> transform -> export -> push) is workflow-agnostic.
Optional workflow-specific case builders run between transform and export to
enrich the data warehouse with a case table before the final export.
"""

import os
import sys
import subprocess
import logging

# Configure clean, lightweight console logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

def main():
    # Ensure execution context matches the script's native directory to avoid path conflicts
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Generic process mining pipeline - works for any n8n workflow
    pipeline_scripts = [
        "01_extract_data_collector.py",
        "02_transform_process_pipeline.py",
        # Optional case-builder step. Comment out or replace with a different
        # workflow-specific script (e.g. optional_case_log_ticketing.py) as needed.
        "optional_case_log_invoice.py",
        "03_load_event_exporter.py",
        "04_push_to_celonis.py"
    ]

    logging.info("=== STARTING ETL PIPELINE ===")

    for script in pipeline_scripts:
        # Skip optional steps gracefully if the corresponding file has been removed
        if script.startswith("optional_") and not os.path.exists(script):
            logging.info(f"Skipping optional step (not present): {script}")
            continue

        logging.info(f"Executing: {script}")

        # Execute sub-process and stream its logs directly to stdout
        result = subprocess.run([sys.executable, script])

        # Fail-Fast mechanism: Immediately abort the entire chain if a phase fails
        if result.returncode != 0:
            logging.error(f"PIPELINE ABORTED: {script} failed (Exit Code: {result.returncode})")
            sys.exit(1)

    logging.info("=== PIPELINE SUCCESSFUL | DATA IS LIVE IN CELONIS ===")

if __name__ == "__main__":
    main()
