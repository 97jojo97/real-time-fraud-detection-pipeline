# Real-Time Fraud Detection Data Pipeline

A production-grade, event-driven streaming pipeline designed to classify fraudulent credit card transactions in real-time. This project simulates a live payment network, enriching high-velocity streams with a massive historical Data Warehouse.

## 🚀 Key Features

*   **Real-Time Streaming**: Uses **Apache Kafka** to simulate high-throughput POS terminal swipes.
*   **Hybrid Data Architecture**: Combines a stream of "Live Events" with a **9-Million Row** PostgreSQL Data Warehouse for enrichment.
*   **High-Speed In-Memory Caching**: Implements an O(1) lookup "Brain" in Python that caches 8.9 million fraud labels for micro-second decision making.
*   **Analytics Dashboard**: Advanced SQL queries showcasing data extraction for bank risk mitigation and fraud trends.
*   **Dockerized Infrastructure**: Entire environment (Kafka, Zookeeper, PostgreSQL) launches with one command.

## 🛠️ Technology Stack

*   **Language**: Python 3.10+
*   **Infrastructure**: Docker, Docker Compose
*   **Event Broker**: Apache Kafka
*   **Data Warehouse**: PostgreSQL 15 (Alpine)
*   **ETL Library**: Pandas, SQLAlchemy
*   **Data Source**: local files: 13,305,915 transactions + 8,914,963 fraud labels from kaggle datasets

## 📁 Repository Structure

*   `data/`: Raw dataset files (CSV/JSON). (Excluded from repository)
*   `scripts/load_dimensions.py`: Batch Ingestion engine for dimension warehouse.
*   `scripts/producer.py`: The Mock POS terminal producing live streams.
*   `scripts/master_sink.py`: The detection brain consuming streams and sinking results into PostgreSQL.
*   `scripts/analytics_report.py`: Analytical engine for high-level business intelligence.

## 🏁 Quick Start

1.  **Launch Servers**:
    ```bash
    docker-compose up -d
    ```
2.  **Ingest Warehouse (9M Records)**:
    ```bash
    python scripts/load_dimensions.py
    ```
3.  **Run Pipeline (Live Detect)**:
    Start the consumer/sink (the brain):
    ```bash
    python scripts/master_sink.py
    ```
    (In another terminal) Start the producer (the card-swipes):
    ```bash
    python scripts/producer.py --limit 10000
    ```
4.  **Run Analytical Report**:
    ```bash
    python scripts/analytics_report.py
    ```

---
*Created as a comprehensive showcase of Data Engineering and Real-Time Pipeline skills.*
