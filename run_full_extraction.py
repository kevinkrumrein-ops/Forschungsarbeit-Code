"""
Module 00: ETL Pipeline Orchestrator
------------------------------------
Sequentially orchestrates the execution of the n8n Process Mining ETL pipeline.
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

    # Sequential pipeline definition (Data-driven architecture)
    pipeline_scripts = [
        "01_extract_data_collector.py",
        "02_transform_process_pipeline.py",
        "03_load_event_exporter.py",
        "04_push_to_celonis.py"
    ]

    logging.info("=== STARTING ETL PIPELINE ===")

    for script in pipeline_scripts:
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