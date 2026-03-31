import pandas as pd
import os
import json

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

def inspect_csv(file_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return False
    
    print(f"\n{'='*60}")
    print(f"[+] Inspecting CSV: {file_name}")
    print(f"{'='*60}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"[*] Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        
        print("\n--- Data Types ---")
        print(df.dtypes)
        
        print("\n--- Missing Value Counts ---")
        null_counts = df.isnull().sum()
        missing = null_counts[null_counts > 0]
        if len(missing) == 0:
            print("No missing values found.")
        else:
            print(missing)
            
        print("\n--- Unique Keys Check ---")
        id_cols = [col for col in df.columns if any(substring in col.lower() for substring in ['id', 'code'])]
        if id_cols:
            for col in id_cols:
                print(f" - {col}: {df[col].nunique()} unique values")
        else:
            print("No obvious ID columns found based on naming conventions.")

        print("\n--- Top 3 Sample Rows ---")
        print(df.head(3).to_string())
        return True
    except Exception as e:
        print(f"[-] Error reading {file_name}: {e}")
        return False

def inspect_json(file_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return False
    
    print(f"\n{'='*60}")
    print(f"[+] Inspecting JSON: {file_name}")
    print(f"{'='*60}")

    try:
        try:
            df = pd.read_json(file_path)
            print(f"[*] Parsed as typical DataFrame -> Shape: {df.shape}")
        except ValueError:
            df = pd.read_json(file_path, lines=True)
            print(f"[*] Parsed as JSON Lines -> Shape: {df.shape}")

        print("\n--- Data Types ---")
        print(df.dtypes)

        print("\n--- Top 3 Sample Rows ---")
        print(df.head(3).to_string())
        return True
    except Exception as e:
        print(f"[-] Pandas parse failed: {e}. Falling back to native json library...")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    print(f"Loaded a Dictionary with {len(data.keys())} keys.")
                    sample_key = list(data.keys())[0] if len(data) > 0 else None
                    print(f"Sample Entry -> {sample_key}: {data.get(sample_key)}")
                elif isinstance(data, list):
                    print(f"Loaded a List with {len(data)} items.")
                    print(f"Sample Entry -> {data[0] if len(data) > 0 else 'Empty'}")
            return True
        except Exception as inner_e:
            print(f"[-] Native parse failed: {inner_e}")
            return False

def run_inspections():
    print(f"Looking for dataset files in: {os.path.abspath(DATA_DIR)}")
    
    found_transactions = inspect_csv('transactions_data.csv')
    if not found_transactions:
        print("\n[-] Missing: transactions_data.csv")

    found_cards = inspect_csv('cards_dat.csv') or inspect_csv('cards_data.csv')
    if not found_cards:
        print("\n[-] Missing: cards_dat.csv or cards_data.csv")

    found_Users = inspect_csv('users_data.csv') or inspect_csv('users_data')
    if not found_Users:
        print("\n[-] Missing: users_data (or users_data.csv)")

    found_mcc = inspect_json('mcc_codes.json')
    if not found_mcc:
        print("\n[-] Missing: mcc_codes.json")

    found_fraud = inspect_json('train_fraud_labels.json')
    if not found_fraud:
        print("\n[-] Missing: train_fraud_labels.json")

if __name__ == "__main__":
    run_inspections()
