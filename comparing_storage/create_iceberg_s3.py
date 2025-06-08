"""
PySpark script to create an Apache Iceberg table in a standard S3 bucket.

This script performs the following actions:
1.  Initializes a Spark Session with configurations for Apache Iceberg and AWS Glue
    Data Catalog.
2.  Reads a source Parquet dataset from an S3 bucket.
3.  Writes the data into a new or existing Iceberg table, partitioned by year and quarter.
4.  The Iceberg table's metadata is managed by the AWS Glue Data Catalog, and its
    data files are stored in a specified S3 warehouse location.

Prerequisites:
- A running Spark environment (e.g., on an EC2 instance) with Spark 4.0.
- AWS credentials configured in the environment where Spark is running, with
  permissions for S3 (read/write) and AWS Glue (create/update tables/databases).
- The source Parquet data should be available in the specified S3 path.

Usage:
    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0 \\
        create_iceberg_s3.py \\
        --warehouse-path s3a://your-iceberg-s3-warehouse-bucket/warehouse \\
        --source-path s3a://your-source-data-bucket/path/to/parquet-data \\
        --catalog-name glue_catalog \\
        --db-name iceberg_benchmark_db \\
        --table-name s3_standard_table \\
        --partition-cols year,quarter

"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract

def parse_arguments():
    """
    Parses command-line arguments for the script.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Create an Iceberg table on S3 from Parquet data.")
    parser.add_argument("--warehouse-path", type=str, required=True,
                        help="S3 path for the Iceberg warehouse (e.g., s3a://your-bucket/warehouse)")
    parser.add_argument("--source-path", type=str, required=True,
                        help="S3 path to the source Parquet dataset (e.g., s3a://your-bucket/data)")
    parser.add_argument("--catalog-name", type=str, default="glue_catalog",
                        help="Name of the Iceberg catalog (default: glue_catalog)")
    parser.add_argument("--db-name", type=str, required=True,
                        help="Name of the database in the catalog (e.g., iceberg_benchmark_db)")
    parser.add_argument("--table-name", type=str, required=True,
                        help="Name of the Iceberg table to create (e.g., s3_standard_table)")
    parser.add_argument("--partition-cols", type=str, default="year,quarter",
                        help="Comma-separated list of column names to partition the table by (e.g., year,quarter)")

    args = parser.parse_args()
    # Split the partition columns string into a list.
    args.partition_cols = [col.strip() for col in args.partition_cols.split(",")]
    return args

def main():
    """
    Main function to create and run the Spark job for creating an Iceberg table on S3.
    """
    args = parse_arguments()
    print("Starting Spark job to create Iceberg table on S3.")
    print(f"Arguments received: {args}")

    spark = (
        SparkSession.builder.appName("S3-Standard-Iceberg-Creation")
        # --- Iceberg SQL Extensions ---
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # --- Iceberg Catalog Configuration (for AWS Glue) ---
        # Use SparkCatalog as the main entry point and specify Glue for the implementation
        .config(
            f"spark.sql.catalog.{args.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{args.catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{args.catalog_name}.warehouse", args.warehouse_path
        )
        .config(
            f"spark.sql.catalog.{args.catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # --- Performance Tuning for a single c5.4xlarge node (16 vCPU, 32GB RAM) ---
        # This configuration assumes a standalone Spark cluster on the single node.
        .config("spark.driver.memory", "20g")
        .config("spark.executor.memory", "20g")
        # Leave a couple of cores for the OS and the Spark driver process
        .config("spark.executor.cores", "14")
        .config("spark.sql.shuffle.partitions", "200")  # Good starting point for 110GB
        .getOrCreate()
    )

    print("Spark session created successfully with Iceberg support.")

    # 1. Create the database in AWS Glue Data Catalog if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.db_name}")
    print(f"Database '{args.db_name}' is ready in catalog '{args.catalog_name}'.")

    # 2. Read the source Parquet data from S3
    # print(f"Reading Parquet data from: {args.source_path}")
    try:
        # Use a glob path pattern to be specific about which files to read.
        # This helps Spark find the files in the nested directory structure.
        source_df = spark.read.parquet(f"{args.source_path}/*/*/*.parquet")

        # Since partition columns (year, quarter) are in the file path and not in the
        # data itself, we must extract them to create new columns in the DataFrame.
        # This is necessary for the .partitionBy() step later.
        print("Extracting partition columns from file path...")
        # The regex patterns below assume a path structure like '.../2023/q1/some-file.parquet'
        source_df = source_df.withColumn("input_file", input_file_name()) \
            .withColumn("year", regexp_extract("input_file", r"/(\d{4})/", 1)) \
            .withColumn("quarter", regexp_extract("input_file", r"/(q\d)/", 1)) \
            .drop("input_file")

        print("Successfully read source data. Schema with new partition columns:")
        source_df.printSchema()
        print(f"Total rows read from source: {source_df.count()}")
    except Exception as e:
        print(f"Error reading from source S3 path: {args.source_path}")
        print("Please check if the path is correct and you have the necessary permissions.")
        print(f"Error details: {e}")
        spark.stop()
        return


    # 3. Write the DataFrame to an Iceberg table
    table_identifier = f"{args.catalog_name}.{args.db_name}.{args.table_name}"
    print(f"Writing data to Iceberg table: {table_identifier}")

    # Partitioning is crucial for query performance. Use the columns provided via args.
    try:
        (
            source_df.write.format("iceberg")
            .mode("overwrite") # Use "overwrite" to recreate the table, or "append" to add data
            .partitionBy(*args.partition_cols) # Use the partition columns from args
            .save(table_identifier)
        )
        print(f"Successfully wrote data to Iceberg table '{table_identifier}'.")
    except Exception as e:
        print(f"Error writing to Iceberg table '{table_identifier}'.")
        print(f"This could be due to partitioning columns {args.partition_cols} not existing in the DataFrame.")
        print(f"Error details: {e}")
        spark.stop()
        return

    # 4. (Optional) Read from the Iceberg table to verify the write operation
    print("\nVerification step: Reading data back from the Iceberg table.")
    try:
        iceberg_df = spark.table(table_identifier)
        print(f"Successfully read from '{table_identifier}'.")
        print(f"Verification count: {iceberg_df.count()} rows.")
        print("Showing a sample of 5 rows from the Iceberg table:")
        iceberg_df.show(5)
    except Exception as e:
        print(f"Could not verify by reading from the Iceberg table. Error: {e}")


    print("\nSpark job finished.")
    spark.stop()


if __name__ == "__main__":
    main() 