import subprocess
import sys
import os
import time
from datetime import datetime

def run_script(script_name):
    """
    Executes a sub-process using the current Python interpreter.
    Silent mode: Logic relies on the sub-scripts' internal professional logging.
    """
    result = subprocess.run([sys.executable, script_name], capture_output=False, text=True)
    
    if result.returncode == 0:
        return True
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ORCHESTRATOR] [ERROR] Phase {script_name} failed (Exit Code: {result.returncode})")
        return False

def main():
    """
    Main orchestration loop. Ensures directory integrity and manages the 
    sequential ETL (Extract, Transform, Load) execution chain.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("="*70)
    print("N8N PROCESS MINING: FULL ETL EXTRACTION SEQUENCE")
    print("="*70)

    # Phase 1: Data Acquisition (Extract)
    if run_script("01_extract_data_collector.py"):
        
        # Phase 2: Logical Processing (Transform)
        if run_script("02_transform_process_pipeline.py"):
            
            # Phase 3: Result Persistence (Load)
            if run_script("03_load_event_exporter.py"):
                print("\n" + "="*70)
                print("ETL PIPELINE SUCCESSFUL | READY FOR ANALYSIS")
                print("="*70)
            else:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [ABORT] Phase 03_load failed.")
        else:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [ABORT] Phase 02_transform failed.")
    else:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [ABORT] Phase 01_extract failed.")

if __name__ == "__main__":
    main()