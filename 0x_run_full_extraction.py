"""
Module 00: ETL Pipeline Orchestrator
------------------------------------
Runs the n8n process mining ETL stages in order, each as a separate subprocess.
If a stage exits with a non-zero code, the pipeline aborts immediately.
"""

import os
import sys
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("PIPELINE")

# Stages in execution order. The case builder (02b) always runs between transform and export, so a case log is produced for every workflow.
PIPELINE_SCRIPTS = [
    "01_extract_data_collector.py",
    "02a_transform_event_log.py",
    "02b_transform_case_log.py",
    "03_load_event_exporter.py",
    "04_push_to_celonis.py",
]

def main():
    # Run from the script's own directory so relative paths resolve consistently.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    logger.info("=== STARTING ETL PIPELINE ===")

    for script in PIPELINE_SCRIPTS:
        logger.info(f"Running {script}")
        result = subprocess.run([sys.executable, script])
        if result.returncode != 0:
            logger.error(f"Pipeline aborted: {script} failed (exit code {result.returncode})")
            sys.exit(1)

    logger.info("=== PIPELINE SUCCESSFUL | DATA IS LIVE IN CELONIS ===")

if __name__ == "__main__":
    main()