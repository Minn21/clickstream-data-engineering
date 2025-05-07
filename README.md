# Clickstream Data Project

This project demonstrates a complete data engineering pipeline that processes website clickstream data to analyze popular pages and user dwell time.

## Tech Stack

- **Ingestion**: Kafka + Python producer
- **Processing**: PySpark (batch)
- **Orchestration**: Airflow
- **Storage**: SQLite
- **Infrastructure**: Docker
- **Validation**: Great Expectations
- **Visualization**: Metabase

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── clickstream_dag.py
│   └── data/
├── spark-apps/
│   ├── clickstream_processor.py
│   └── data_generator.py
├── spark-data/
├── metabase-data/
├── sqlite-data/
│   └── clickstream.db
├── docker-compose.yml
└── README.md
```

## Setup Guide

### 1. Clone the Repository

```bash
git clone <repository-url>
cd clickstream-data-engineering
```

### 2. Start the Services

```bash
docker-compose up -d
```

This will start all required services:
- Kafka & Zookeeper (data ingestion)
- Spark Master & Worker (data processing)
- Airflow (workflow orchestration)
- Metabase (data visualization)

### 3. Access the Services

- **Airflow**: http://localhost:8081 (username: admin, password: admin)
  - The DAG will run automatically every day
  - You can also trigger it manually

- **Spark Master UI**: http://localhost:8080
  - Monitor Spark jobs

- **Metabase**: http://localhost:3000
  - Set up a new account
  - Connect to SQLite database at `/sqlite-data/clickstream.db`
  - Create dashboards to visualize page popularity and dwell time

## Pipeline Details

1. **Data Generation**: Python script generates mock clickstream data and sends it to Kafka
2. **Data Processing**: PySpark job consumes data from Kafka, calculates average dwell time per page
3. **Data Validation**: Great Expectations validates data quality (no negative durations)
4. **Data Storage**: Results stored in SQLite database
5. **Data Visualization**: Metabase dashboard shows popular pages and dwell times
