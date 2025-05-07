#!/usr/bin/env python3
"""
Clickstream Data Processor

This PySpark script processes clickstream data from Kafka,
calculates average dwell time per page, and stores results in SQLite.
"""

import os
import sys
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC = 'clickstream'
KAFKA_BROKER = 'kafka:29092'
SQLITE_DB_PATH = '/sqlite-data/clickstream.db'

def create_spark_session():
    """
    Create and return a Spark session
    """
    return SparkSession.builder \
        .appName("ClickstreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

def read_from_kafka(spark):
    """
    Read clickstream data from Kafka
    
    Args:
        spark: Spark session
        
    Returns:
        DataFrame with clickstream data
    """
    # Define schema for the clickstream data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("duration_sec", IntegerType(), True)
    ])
    
    # Read from Kafka
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Convert the value column from Kafka (which contains our JSON data)
    from pyspark.sql.functions import from_json, col
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df

def process_clickstream_data(df):
    """
    Process clickstream data to calculate average dwell time per page
    
    Args:
        df: DataFrame with clickstream data
        
    Returns:
        DataFrame with processed results
    """
    # Filter out negative durations
    valid_df = df.filter(col("duration_sec") >= 0)
    
    # Calculate metrics per page
    result_df = valid_df.groupBy("page_url") \
        .agg(
            count("*").alias("visit_count"),
            avg("duration_sec").alias("avg_duration_sec")
        ) \
        .orderBy(col("visit_count").desc())
    
    return result_df

def save_to_sqlite(df, table_name="page_metrics"):
    """
    Save DataFrame to SQLite database
    
    Args:
        df: DataFrame to save
        table_name: Name of the table to save to
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
    
    # Convert to Pandas DataFrame for easy SQLite insertion
    pandas_df = df.toPandas()
    
    # Connect to SQLite and save data
    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        pandas_df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"Successfully saved data to SQLite table '{table_name}'")
    except Exception as e:
        logger.error(f"Error saving to SQLite: {e}")
    finally:
        conn.close()

def validate_data(df):
    """
    Validate data quality using simple checks
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Boolean indicating if validation passed
    """
    # Check 1: No negative durations
    negative_count = df.filter(col("duration_sec") < 0).count()
    if negative_count > 0:
        logger.warning(f"Found {negative_count} records with negative duration")
        return False
    
    # Check 2: Ensure we have data
    if df.count() == 0:
        logger.warning("No data found in the DataFrame")
        return False
    
    # Check 3: Ensure all required columns are present
    required_columns = ["user_id", "page_url", "timestamp", "duration_sec"]
    for column in required_columns:
        if column not in df.columns:
            logger.warning(f"Required column '{column}' is missing")
            return False
    
    logger.info("All data validation checks passed")
    return True

def main():
    """
    Main function to process clickstream data
    """
    try:
        logger.info("Starting clickstream data processor")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Read data from Kafka
        logger.info(f"Reading data from Kafka topic '{KAFKA_TOPIC}'")
        clickstream_df = read_from_kafka(spark)
        
        # Validate data
        logger.info("Validating data quality")
        is_valid = validate_data(clickstream_df)
        if not is_valid:
            logger.warning("Data validation failed, but continuing with processing")
        
        # Process data
        logger.info("Processing clickstream data")
        result_df = process_clickstream_data(clickstream_df)
        
        # Show results
        logger.info("Processed results:")
        result_df.show(truncate=False)
        
        # Save to SQLite
        logger.info(f"Saving results to SQLite database at {SQLITE_DB_PATH}")
        save_to_sqlite(result_df)
        
        logger.info("Clickstream data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in clickstream data processor: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())