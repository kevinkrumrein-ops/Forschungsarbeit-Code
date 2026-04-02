import json
import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    user = "user"
    pw = "password"
    db = "db"
    host = "127.0.0.1"
    return create_engine(f"postgresql://{user}:{pw}@{host}:5434/{db}")

def run_deep_debug():
    db_engine = get_engine()
    query = """
        SELECT e.id, d.data 
        FROM raw_execution_entity e 
        JOIN raw_execution_data d ON e.id = d."executionId"
        LIMIT 1
    """
    
    df = pd.read_sql(query, db_engine)
    raw_data = df.iloc[0]['data']
    g = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
    
    print("--- DEEP DIVE POINTER DEBUGGING ---")
    
    # 1. Den Pointer zur Mapping-Tabelle auslesen (wir wissen jetzt, es ist die 5)
    run_data_ptr = int(g[2].get('runData'))
    mapping = g[run_data_ptr]
    
    print(f"1. Mapping-Tabelle (Index {run_data_ptr}) Datentyp: {type(mapping)}")
    
    if isinstance(mapping, dict):
        print(f"2. Gefundene Nodes (Anzahl): {len(mapping.keys())}")
        print(f"   Node Namen: {list(mapping.keys())[:5]} ...") # Zeige nur die ersten 5
        
        # 3. Wir testen den Pfad an der allerersten gefundenen Node
        first_node = list(mapping.keys())[0]
        run_list_ptr = int(mapping[first_node])
        run_list = g[run_list_ptr]
        
        print(f"\n--- TEST FÜR NODE: '{first_node}' ---")
        print(f"4. Run-Verzeichnis (Index {run_list_ptr}) Datentyp: {type(run_list)}")
        
        if isinstance(run_list, list) and len(run_list) > 0:
            meta_ptr = int(run_list[0])
            meta_obj = g[meta_ptr]
            print(f"5. Metadaten-Objekt (Index {meta_ptr}) Datentyp: {type(meta_obj)}")
            
            # Zeige die Keys des Metadaten-Objekts, um zu sehen, ob startTime etc. existieren
            if isinstance(meta_obj, dict):
                print(f"   Vorhandene Metadaten-Keys: {list(meta_obj.keys())}")
                
                if 'data' in meta_obj:
                    payload_ptr = int(meta_obj['data'])
                    payload_obj = g[payload_ptr]
                    print(f"6. Payload-Objekt (Index {payload_ptr}) Datentyp: {type(payload_obj)}")
                else:
                    print("FEHLER: Metadaten-Objekt hat keinen 'data'-Pointer!")
            else:
                print("FEHLER: Metadaten-Objekt ist kein Dictionary!")
        else:
            print("FEHLER: Run-Verzeichnis ist keine Liste oder leer!")
    else:
        print("FEHLER: Mapping-Tabelle ist kein Dictionary!")

if __name__ == "__main__":
    run_deep_debug()