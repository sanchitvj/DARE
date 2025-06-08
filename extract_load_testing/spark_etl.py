import argparse
import logging
import os
import re
from datetime import datetime
import time
import json
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, input_file_name, udf
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_s3_file_stats(bucket_name, prefix=""):
    """Lists files in S3 and returns their keys and total size."""
    s3 = boto3.client('s3')
    keys = []
    total_size = 0
    paginator = s3.get_paginator('list_objects_v2')
    try:
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for page in pages:
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.csv'):
                        keys.append(obj['Key'])
                        total_size += obj['Size']
    except Exception as e:
        logging.error(f"Could not list objects in bucket '{bucket_name}': {e}")
        raise
    return keys, total_size


def extract_metadata_py(file_path):
    """
    Python function to replicate the Go program's metadata extraction.
    Extracts date and other metadata from the file path.
    """
    try:
        filename = os.path.basename(file_path)
        filename_no_ext = os.path.splitext(filename)[0]

        parsed_date = None

        # A list of common date formats to try
        date_formats = [
            "%Y%m%d",     # 20240315
            "%Y-%m-%d",   # 2024-03-15
            "%Y_%m_%d",   # 2024_03_15
            "%m-%d-%Y",   # 03-15-2024
            "%m_%d_%Y",   # 03_15_2024
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(filename_no_ext, fmt)
                break
            except ValueError:
                continue

        # If filename parsing failed, try to extract year from path
        if not parsed_date:
            # Regex to find a year in the path, e.g., /data_2024/ or /data_Q1_2023/
            year_regex = re.compile(r'data_(?:Q[1-4]_)?(\d{4})')
            match = year_regex.search(file_path)
            if match:
                year = int(match.group(1))
                # Use January 1st as a default date if only year is found
                parsed_date = datetime(year, 1, 1)
                logging.info(
                    f"Extracted year {year} from path '{file_path}', using default date.")

        if not parsed_date:
            logging.warning(
                f"Could not parse date from filename or path: {file_path}")
            return None

        # Extract year, quarter, month
        year = parsed_date.year
        month = parsed_date.month
        quarter = ((month - 1) // 3) + 1
        quarter_str = f"q{quarter}"
        date_str = parsed_date.strftime("%Y-%m-%d")

        return (date_str, year, quarter_str, month)

    except Exception as e:
        logging.error(f"Error extracting metadata from {file_path}: {e}")
        return None


def process_files(spark, source_bucket, target_bucket):
    """
    Main ETL processing function.
    """
    start_time = time.time()
    logging.info("Starting Spark ETL pipeline...")
    logging.info(f"Source bucket: {source_bucket}")
    logging.info(f"Target bucket: {target_bucket}")

    # Get file stats from S3 before processing
    logging.info(f"Getting file stats from S3 bucket: {source_bucket}")
    all_keys, total_bytes = get_s3_file_stats(source_bucket)
    total_files_found = len(all_keys)

    if total_files_found == 0:
        logging.warning("No CSV files found in source bucket. Exiting.")
        return

    source_path = f"s3a://{source_bucket}/**/*.csv"
    target_path = f"s3a://{target_bucket}/"

    # Define the schema for the UDF's return type
    metadata_schema = StructType([
        StructField("date", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", StringType(), False),
        StructField("month", IntegerType(), False)
    ])

    # Register the Python function as a Spark UDF
    extract_metadata_udf = udf(extract_metadata_py, metadata_schema)

    # Read all CSV files from the source path.
    # Spark handles discovery of files and distribution of work.
    # We allow for a variable number of columns by reading each line as a single text column.
    # This is a common approach when schema is unknown or varied.
    # A better approach if schema is consistent is to provide it.
    try:
        df = spark.read.option("header", "false").csv(source_path)
        logging.info("Successfully read CSV files from S3.")
    except Exception as e:
        logging.error(f"Failed to read CSV files from {source_path}. Error: {e}")
        # In a real-world scenario, you might want to check if the path exists or if there are any files.
        # For this example, we assume files are present. If not, Spark will raise an AnalysisException.
        if "Path does not exist" in str(e):
             logging.warning(f"No CSV files found at path: {source_path}. Exiting.")
             return
        raise e


    # Add a column with the source file path for each row
    df = df.withColumn("source_file", input_file_name())

    # Apply the UDF to extract metadata
    df = df.withColumn("metadata", extract_metadata_udf(col("source_file")))

    # Filter out files where metadata extraction failed
    df_filtered = df.filter(col("metadata").isNotNull())

    # Promote the nested struct fields to top-level columns
    df_transformed = df_filtered.select(
        col("metadata.date").alias("date"),
        col("metadata.year").alias("year"),
        col("metadata.quarter").alias("quarter"),
        col("metadata.month").alias("month"),
        col("source_file"),
        "*"  # Keep all original CSV columns
    ).drop("metadata")

    # Get the original csv columns to concatenate them
    # The columns read from CSV are named _c0, _c1, _c2, etc. by default
    csv_cols = [c for c in df.columns if c.startswith('_c')]

    # Create the 'raw_data' column by concatenating all CSV columns with a pipe delimiter
    df_final = df_transformed.withColumn(
        "raw_data", concat_ws("|", *csv_cols)
    ).select(
        "date",
        "year",
        "quarter",
        "month",
        "raw_data",
        "source_file"
    )

    logging.info("Transformation complete. Writing to Parquet...")
    
    # Write the DataFrame to Parquet, partitioned by year and quarter.
    # This is the idiomatic Spark approach for efficient data layout in a data lake.
    # It creates a directory structure like /year=2024/quarter=q1/
    # This differs from the Go program's output of one file per day, but is more scalable.
    df_final.write.partitionBy("year", "quarter").mode(
        "overwrite").parquet(target_path)

    logging.info(f"Successfully wrote data to {target_path}")

    # Collect stats after processing
    duration = time.time() - start_time
    total_rows_processed = df_final.count()
    # Count files that were successfully processed and included in the final output
    files_processed_count = df_final.select("source_file").distinct().count()
    files_failed_or_skipped = total_files_found - files_processed_count

    stats = {
        "total_execution_time": f"{duration:.4f}s",
        "total_files_found": total_files_found,
        "files_processed": files_processed_count,
        "files_failed_or_skipped": files_failed_or_skipped,
        "total_rows_processed": total_rows_processed,
        "total_bytes_processed": total_bytes,
        "processing_throughput_gb_per_sec": (total_bytes / 1e9) / duration if duration > 0 else 0,
    }

    stats_path = "etl_stats.json"
    try:
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=4)
        logging.info(f"Successfully wrote stats to {stats_path}")
    except Exception as e:
        logging.error(f"Failed to write stats file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="PySpark ETL job to process CSV files from S3 to Parquet.")
    parser.add_argument("source_bucket", help="Source S3 bucket name.")
    parser.add_argument("target_bucket", help="Target S3 bucket name.")
    args = parser.parse_args()

    spark = None
    try:
        # NOTE: To run this against S3, your Spark environment needs to be
        # configured with AWS credentials and the hadoop-aws JAR.
        # Example for spark-submit:
        # spark-submit --packages org.apache.hadoop:hadoop-aws:3.2.0 spark_etl.py <source> <target>
        spark = SparkSession.builder \
            .appName("S3-CSV-to-Parquet-ETL") \
            .master("local[16]") \
            .config("spark.driver.memory", "24g") \
            .config("spark.sql.shuffle.partitions", "64") \
            .getOrCreate()

        process_files(spark, args.source_bucket, args.target_bucket)

    except Exception as e:
        logging.fatal(f"An error occurred in the Spark ETL job: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")


if __name__ == "__main__":
    main() 