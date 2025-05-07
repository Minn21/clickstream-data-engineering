#!/usr/bin/env python3
"""
Clickstream Data Pipeline DAG

This Airflow DAG orchestrates the clickstream data pipeline with three main tasks:
1. Generate mock clickstream data and send to Kafka
2. Process clickstream data with PySpark
3. Validate data quality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sqlite3
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'clickstream_pipeline',
    default_args=default_args,
    description='Clickstream data processing pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['clickstream', 'data-engineering'],
)

# Task 1: Generate clickstream data and send to Kafka
generate_data = BashOperator(
    task_id='generate_data',
    bash_command='python /opt/spark-apps/data_generator.py',
    dag=dag,
)

# Task 2: Process clickstream data with PySpark
process_data = BashOperator(
    task_id='process_data',
    bash_command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/clickstream_processor.py',
    dag=dag,
)

# Task 3: Validate data quality
def validate_data_quality():
    """
    Validate data quality using Great Expectations-inspired checks
    """
    # Connect to SQLite database
    db_path = '/sqlite-data/clickstream.db'
    conn = sqlite3.connect(db_path)
    
    # Read the page_metrics table
    query = "SELECT * FROM page_metrics"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Validation 1: Check if we have data
    assert len(df) > 0, "No data found in page_metrics table"
    
    # Validation 2: Check if avg_duration_sec is always positive
    assert (df['avg_duration_sec'] >= 0).all(), "Found negative duration values"
    
    # Validation 3: Check if required columns exist
    required_columns = ['page_url', 'visit_count', 'avg_duration_sec']
    for col in required_columns:
        assert col in df.columns, f"Required column '{col}' is missing"
    
    print("All data quality checks passed!")
    return True

validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_quality,
    dag=dag,
)

# Set task dependencies
generate_data >> process_data >> validate_data