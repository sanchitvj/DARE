import argparse
import logging
import os
import re
from datetime import datetime
import time
import json
import boto3

import polars as pl
from polars import Utf8

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


def extract_metadata_py(file_path: str) -> tuple | None:
    """
    Python function to replicate the Go program's metadata extraction.
    Extracts date and other metadata from the file path.
    Designed to be applied to a Polars Series.
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
            "%m_%d-%Y",   # typo format
            "%m_%d_%Y",   # 03_15_2024
        ]

        for fmt in date_formats:
            try:
                # Use datetime.strptime for robust date parsing
                parsed_date = datetime.strptime(filename_no_ext, fmt)
                break
            except ValueError:
                continue

        # If filename parsing failed, try to extract year from path
        if not parsed_date:
            year_regex = re.compile(r'data_(?:Q[1-4]_)?(\d{4})')
            match = year_regex.search(file_path)
            if match:
                year = int(match.group(1))
                # Use January 1st as a default date
                parsed_date = datetime(year, 1, 1)
                logging.info(f"Extracted year {year} from path '{file_path}', using default date.")

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


def process_files_with_polars(source_bucket: str, target_bucket: str):
    """
    Main ETL processing function using Polars.
    """
    start_time = time.time()
    logging.info("Starting Polars ETL pipeline...")
    logging.info(f"Source bucket: {source_bucket}")
    logging.info(f"Target bucket: {target_bucket}")

    # Get file stats from S3 for an accurate byte count
    logging.info(f"Getting file stats from S3 bucket: {source_bucket}")
    all_keys, total_bytes = get_s3_file_stats(source_bucket)
    if not all_keys:
        logging.warning("No CSV files found in source bucket. Exiting.")
        return

    source_path = f"s3://{source_bucket}/**/*.csv"
    target_path = f"s3://{target_bucket}/"

    try:
        # Lazily scan all CSV files. Polars handles the glob pattern and
        # reads the files in parallel and partitioned.
        # `scan_csv` is memory-efficient as it doesn't load all data at once.
        lazy_df = pl.scan_csv(source_path, has_header=False, with_paths=True)
        logging.info(f"Successfully started scanning files from {source_path}")
    except Exception as e:
        # Polars might raise a generic ComputeError if path is not found
        logging.error(f"Failed to scan CSV files from {source_path}. Error: {e}")
        if "No files found" in str(e):
             logging.warning(f"No CSV files found at path: {source_path}. Exiting.")
             return
        raise e

    # The schema for the metadata to be extracted.
    metadata_schema = {
        "date": Utf8,
        "year": pl.Int32,
        "quarter": Utf8,
        "month": pl.Int32
    }
    
    # Apply the metadata extraction function to the 'path' column.
    # The `map_elements` function applies a Python function to each element of a series.
    # `collect(streaming=True)` ensures the operation is performed in a memory-efficient way.
    transformed_df = lazy_df.with_columns(
        pl.col("path").map_elements(
            extract_metadata_py,
            return_dtype=pl.Struct(metadata_schema)
        ).alias("metadata")
    ).filter(
        pl.col("metadata").is_not_null()
    ).unnest(
        "metadata"
    ).rename(
        {"path": "source_file"}
    )
    
    # Concatenate all original columns into a single 'raw_data' column
    # The columns read from CSV are named column_1, column_2, etc. by default
    # csv_cols = [col for col in transformed_df.columns if col.startswith('column_')]
    
    final_df = transformed_df.with_columns(
        pl.concat_str(pl.all().exclude(["source_file", "date", "year", "quarter", "month"]), separator="|").alias("raw_data")
    ).select(
        "date",
        "year",
        "quarter",
        "month",
        "raw_data",
        "source_file"
    )

    logging.info("Transformation complete. Collecting stats and writing to Parquet...")

    # To get stats before sinking, we need to run computations on the lazy frame.
    # This is efficient as it only computes the aggregates, not the full dataset.
    stats_df = final_df.group_by(pl.lit(1)).agg(
        pl.count().alias("total_rows_processed"),
        pl.col("source_file").n_unique().alias("files_processed_count")
    ).collect()

    total_rows_processed = stats_df["total_rows_processed"][0]
    files_processed_count = stats_df["files_processed_count"][0]
    total_files_found = len(all_keys)
    files_failed_or_skipped = total_files_found - files_processed_count

    # Sink the lazy dataframe to a partitioned parquet dataset.
    # This is the primary, heavy operation.
    final_df.sink_parquet(
        target_path,
        partition_on=["year", "quarter"],
        row_group_size=250_000 # Good practice to define for better read performance
    )
    
    duration = time.time() - start_time
    logging.info(f"Successfully wrote data to {target_path}")

    # Create and write stats
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
        description="Polars ETL job to process CSV files from S3 to Parquet.")
    parser.add_argument("source_bucket", help="Source S3 bucket name.")
    parser.add_argument("target_bucket", help="Target S3 bucket name.")
    args = parser.parse_args()

    try:
        process_files_with_polars(args.source_bucket, args.target_bucket)
    except Exception as e:
        logging.fatal(f"An error occurred in the Polars ETL job: {e}", exc_info=True)


if __name__ == "__main__":
    main() 