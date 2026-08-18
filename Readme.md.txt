# Real-Time Reddit Sentiment Analysis Pipeline

An end-to-end distributed data engineering pipeline that ingests, cleans, analyzes, and stores sentiment metrics from high-velocity Reddit streams using Apache Kafka, PySpark Structured Streaming, Apache Airflow, and Amazon S3, fully containerized with Docker.

---

## Project Overview

* **Data Scale:** Ingested and processed 2.7M+ unstructured Reddit post records (324MB raw corpus).
* **Processing Architecture:** Real-time stream ingestion via Kafka -> Distributed NLP text cleaning and classification in PySpark -> Cloud storage persistence in Amazon S3.
* **Storage Optimization:** Converted uncompressed CSV streams into Snappy-compressed columnar Parquet format, reducing storage footprint by ~70%.
* **Orchestration & CI/CD:** Scheduled and monitored via Apache Airflow DAGs, containerized with Docker, and validated using GitHub Actions.

---

## Tech Stack

| Component | Technology |
| :--- | :--- |
| **Language** | Python 3.10 |
| **Streaming & Ingestion** | Apache Kafka (`kafka-python`) |
| **Distributed Processing** | Apache Spark, PySpark Structured Streaming |
| **NLP & Text Hygiene** | NLTK (Tokenization, Stopwords, Regex Cleaning) |
| **Workflow Orchestration** | Apache Airflow |
| **Cloud Storage** | Amazon S3 (Apache Parquet format) |
| **DevOps & Packaging** | Docker, GitHub Actions |

---

## Pipeline Architecture & Data Flow

1. **Ingestion Layer (`kafka_producer.py`):** Reads text records and streams them to a centralized Kafka message topic with controlled throughput.
2. **Stream Processing Layer (`spark_streaming_job.py`):**
   * **Schema Enforcement:** Applies explicit `StructType` definitions to drop corrupted or malformed structural records.
   * **Text Hygiene:** Strips URLs, punctuation, and platform noise, applying NLTK tokenization and stopword removal.
   * **Sentiment Classification:** Computes polarity and categorizes posts into positive, neutral, or negative classifications.
3. **Storage & Optimization:** Streams output partitions into Amazon S3 as Snappy-compressed Parquet files to optimize downstream analytical querying.
4. **Orchestration (`reddit_sentiment_dags.py`):** Airflow DAG schedules pipeline tasks, monitors status, and manages automated retries.

---

## Repository Structure

```text
├── .github/workflows/
│   └── reddit-ci.yml          # GitHub Actions CI workflow (linting & container build)
├── kafka_producer.py          # Data ingestion and streaming script
├── spark_streaming_job.py     # PySpark stream consumer & NLP processing engine
├── reddit_sentiment_dags.py   # Apache Airflow workflow schedule definition
├── requirements.txt           # Python package dependencies
├── Dockerfile                 # Docker container specification
└── README.md                  # Project documentation