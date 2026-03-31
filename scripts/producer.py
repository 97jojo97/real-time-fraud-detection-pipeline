import os
import json
import time

import pandas as pd
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "live_transactions")

# Initialize Kafka Producer (default=str handles numpy/pandas scalars in payloads)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TRANSACTIONS_FILE = os.path.join(DATA_DIR, 'transactions_data.csv')

def run_producer(limit=10000, chunksize=500, sleep_s=0.0):
    """
    Push local CSV rows to Kafka as JSON (mock POS / high-throughput simulation).
    limit: cap rows for iterative testing (implementation plan: 10k first).
    sleep_s: optional per-message delay to tame local runs (0 = fastest).
    """
    print(f"[+] Starting Kafka Producer for topic: {TOPIC_NAME}...")
    total_sent = 0

    for chunk in pd.read_csv(TRANSACTIONS_FILE, chunksize=chunksize):
        for _, row in chunk.iterrows():
            if total_sent >= limit:
                print(f"[!] Reached limit of {limit} rows. Stopping.")
                producer.flush()
                print(f"\n[SUCCESS] Producer finished. Total sent: {total_sent}")
                return

            transaction = row.to_dict()
            producer.send(TOPIC_NAME, transaction)
            total_sent += 1
            if sleep_s > 0:
                time.sleep(sleep_s)

        print(f"    -> Sent {total_sent} transactions...")
        producer.flush()

    print(f"\n[SUCCESS] Producer finished. Total sent: {total_sent}")


if __name__ == "__main__":
    _limit = int(os.environ.get("PRODUCER_LIMIT", "10000"))
    _sleep = float(os.environ.get("PRODUCER_SLEEP_S", "0"))
    run_producer(limit=_limit, sleep_s=_sleep)
