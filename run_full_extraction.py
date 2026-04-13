import subprocess
import sys
import os
import time

def run_script(script_name):
    """Führt ein Python-Skript aus und gibt True zurück, wenn es erfolgreich war."""
    print(f"[{time.strftime('%H:%M:%S')}] Starte: {script_name}...")
    
    # Nutzt den aktuellen Python-Interpreter, um das Skript auszuführen
    result = subprocess.run([sys.executable, script_name], capture_output=False, text=True)
    
    if result.returncode == 0:
        print(f"[{time.strftime('%H:%M:%S')}] {script_name} erfolgreich abgeschlossen.\n")
        return True
    else:
        print(f"[{time.strftime('%H:%M:%S')}] FEHLER: {script_name} ist mit Exit-Code {result.returncode} fehlgeschlagen.\n")
        return False

def main():
    # Sicherstellen, dass wir im Verzeichnis des Skripts arbeiten
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("="*50)
    print("N8N PROCESS MINING: VOLLSTÄNDIGE EXTRAKTION")
    print("="*50)

    # Schritt 1: ETL Pipeline (Daten aus n8n extrahieren und in DB speichern)
    if run_script("process_mining_pipeline.py"):
        
        # Schritt 2: CSV Export (Daten aus DB in CSV exportieren)
        if run_script("export_event_logs.py"):
            print("="*50)
            print("PROZESS ERFOLGREICH BEENDET.")
            print(f"Die Datei 'Event_Logs.csv' liegt bereit in: {script_dir}")
            print("="*50)
        else:
            print("Abbruch: Export fehlgeschlagen.")
    else:
        print("Abbruch: Pipeline fehlgeschlagen. Export wurde nicht gestartet.")

if __name__ == "__main__":
    main()
