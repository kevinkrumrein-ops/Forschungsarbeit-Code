import subprocess
import sys
import os
import time

def run_script(script_name):
    
    print(f"[{time.strftime('%H:%M:%S')}] Launching phase: {script_name}...")
    
    result = subprocess.run([sys.executable, script_name], capture_output=False, text=True)
    
    if result.returncode == 0:
        print(f"[{time.strftime('%H:%M:%S')}] Phase {script_name} completed successfully.\n")
        return True
    else:
        print(f"[{time.strftime('%H:%M:%S')}] ERROR: {script_name} failed with exit code {result.returncode}.\n")
        return False

def main():
    """
    Main orchestration loop. Ensures directory integrity and manages the 
    sequential ETL (Extract, Transform, Load) execution chain.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("="*50)
    print("N8N PROCESS MINING: FULL ETL EXTRACTION SEQUENCE")
    print("="*50)

    # Phase 1: Data Acquisition (Extract)
    if run_script("01_extract_data_collector.py"):
        
        # Phase 2: Logical Processing (Transform)
        if run_script("02_transform_process_pipeline.py"):
            
            # Phase 3: Result Persistence (Load)
            if run_script("03_load_event_exporter.py"):
                print("="*50)
                print("ETL PIPELINE SUCCESSFUL: Event log is ready for analysis.")
                print("="*50)
            else:
                print("ABORT: Phase 03_load failed.")
        else:
            print("ABORT: Phase 02_transform failed.")
    else:
        print("ABORT: Phase 01_extract failed.")

if __name__ == "__main__":
    main()