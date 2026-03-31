"""
Phase 8: Master Transaction Sink (Final Architecture)
======================================================
This script does THREE things:
1. LIVE DETECTION: Predicts fraud for incoming Kafka messages.
2. ENRICHED STORAGE: Saves the ENTIRE transaction record into a new 
   'transactions' table in PostgreSQL.
3. SQL READINESS: This table includes Foreign Keys (client_id, card_id, mcc)
   allowing for advanced JOINs and cross-referencing.
"""

import json
import os
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# Configurations
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "live_transactions")
DB_URI = os.environ.get("DATABASE_URL", "postgresql://admin:admin@localhost:5432/fraud_db")
engine = create_engine(DB_URI)

# Dimension caches (for O(1) in-memory prediction logic)
CARDS_CACHE = {}

def populate_caches():
    print("[+] Loading card risk information from PostgreSQL...")
    with engine.connect() as conn:
        df_cards = pd.read_sql("SELECT id, client_id, card_on_dark_web FROM cards", conn)
    
    global CARDS_CACHE
    CARDS_CACHE = {}
    for _, row in df_cards.iterrows():
        # Composite key: (card_id, client_id)
        CARDS_CACHE[(int(row['id']), int(row['client_id']))] = row['card_on_dark_web']
    print(f"[SUCCESS] {len(CARDS_CACHE)} cards cached for real-time risk checks.")

def init_master_table():
    print("[+] Initializing 'transactions' sink table with Foreign Keys...")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id BIGINT PRIMARY KEY,
                date TIMESTAMP,
                client_id BIGINT,
                card_id BIGINT,
                amount FLOAT,
                use_chip VARCHAR(50),
                merchant_id BIGINT,
                merchant_city VARCHAR(255),
                merchant_state VARCHAR(50),
                zip VARCHAR(20),
                mcc VARCHAR(10),
                errors TEXT,
                predicted_fraud BOOLEAN,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    print("[SUCCESS] 'transactions' table ready for analytical JOINs.")

def predict_fraud(tx):
    """
    Real-Time Prediction Logic.
    Triggers: Card on dark web OR Amount > $500.
    """
    card_key = (int(tx.get('card_id', 0)), int(tx.get('client_id', 0)))
    on_dark_web = CARDS_CACHE.get(card_key) == 'Yes'
    
    try:
        amount_str = str(tx.get('amount', 0)).replace('$', '').replace(',', '')
        amount = float(amount_str)
    except:
        amount = 0.0
        
    return on_dark_web or (amount > 500)

def run_consumer():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"\n[*] MASTER SINK IS LIVE! Listening on topic: {TOPIC_NAME}")
    
    processed_count = 0
    for msg in consumer:
        tx = msg.value
        prediction = predict_fraud(tx)
        
        # MASTER SINK: Ingest the complete record into PostgreSQL
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO transactions (
                        transaction_id, date, client_id, card_id, amount, 
                        use_chip, merchant_id, merchant_city, merchant_state, 
                        zip, mcc, errors, predicted_fraud
                    ) VALUES (
                        :tid, :dt, :cid, :crd, :amt, :chp, :mid, :mcy, :mst, :zp, :mcc, :err, :pred
                    ) ON CONFLICT (transaction_id) DO NOTHING
                """), {
                    "tid": tx.get('id'),
                    "dt": tx.get('date'),
                    "cid": tx.get('client_id'),
                    "crd": tx.get('card_id'),
                    "amt": float(str(tx.get('amount', 0)).replace('$', '').replace(',', '')),
                    "chp": tx.get('use_chip'),
                    "mid": tx.get('merchant_id'),
                    "mcy": tx.get('merchant_city'),
                    "mst": tx.get('merchant_state'),
                    "zp": tx.get('zip'),
                    "mcc": tx.get('mcc'),
                    "err": tx.get('errors'),
                    "pred": bool(prediction)
                })
        except Exception as e:
            print(f"[-] Data Sink Error: {e}")

        processed_count += 1
        if processed_count % 1000 == 0:
            print(f"    -> Ingested {processed_count} live records into PostgreSQL...")
        
        # Stop at 10,000 for our controlled demo
        if processed_count >= 10000:
            print("\n[SUCCESS] Master Sink finished ingesting 10,000 transactions.")
            break

if __name__ == "__main__":
    populate_caches()
    init_master_table()
    run_consumer()
