import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

# DB connection string
DB_URI = "postgresql://admin:admin@127.0.0.1:5432/fraud_db"
engine = create_engine(DB_URI)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

def run_init_sql():
    print("[+] Running init.sql to set up schemas and indexes...")
    init_sql_path = os.path.join(os.path.dirname(__file__), 'init.sql')
    with open(init_sql_path, 'r') as f:
        sql_script = f.read()
    
    with engine.begin() as conn:
        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
    print("[+] init.sql executed securely.")

def load_users():
    print("[+] Loading users_data.csv...")
    df = pd.read_csv(os.path.join(DATA_DIR, 'users_data.csv'))
    df.to_sql('users', engine, if_exists='append', index=False, chunksize=10000)
    print(f"    -> Inserted {len(df)} users.")

def load_cards():
    print("[+] Loading cards_data.csv...")
    df = pd.read_csv(os.path.join(DATA_DIR, 'cards_data.csv'))
    df.to_sql('cards', engine, if_exists='append', index=False, chunksize=10000)
    print(f"    -> Inserted {len(df)} cards.")

def load_mcc():
    print("[+] Loading mcc_codes.json...")
    with open(os.path.join(DATA_DIR, 'mcc_codes.json'), 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(list(data.items()), columns=['mcc', 'description'])
    df.to_sql('mcc_codes', engine, if_exists='append', index=False, chunksize=10000)
    print(f"    -> Inserted {len(df)} MCC codes.")

def load_fraud_labels():
    print("[+] Loading train_fraud_labels.json (Memory Efficient Chunking)...")
    with open(os.path.join(DATA_DIR, 'train_fraud_labels.json'), 'r') as f:
        data = json.load(f)['target']
    
    keys = list(data.keys())
    chunk_size = 100000
    total = len(keys)
    
    for i in range(0, total, chunk_size):
        chunk_keys = keys[i:i + chunk_size]
        chunk_data = [(int(k), data[k]) for k in chunk_keys]
        df = pd.DataFrame(chunk_data, columns=['transaction_id', 'is_fraud'])
        df.to_sql('fraud_labels', engine, if_exists='append', index=False)
        print(f"    -> Inserted batch {i} to {min(i+chunk_size, total)} out of {total} fraud labels.", flush=True)

def main():
    run_init_sql()
    load_users()
    load_cards()
    load_mcc()
    load_fraud_labels()
    print("\n[SUCCESS] Phase 3 Batch Ingestion completed!")

if __name__ == "__main__":
    main()
