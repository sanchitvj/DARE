"""
PySpark script to create or append to an Apache Iceberg table on EMR.

This script is designed to run on a distributed Spark cluster (like EMR)
and handles complex data ingestion scenarios involving messy source directories
and evolving schemas.

Core Logic:
1.  **Manifest-Driven Ingestion:** Instead of scanning unpredictable source
    directories, this script reads a pre-generated 'manifest' file. This
    manifest is a simple CSV that maps every source data file to its correct
    partitioning keys (e.g., year, quarter).
2.  **Batch Processing:** It processes the source files in logical batches,
    grouped by year and quarter from the manifest. This makes the ingestion
    process more reliable and easier to debug.
3.  **Schema Evolution:** The script's primary strength is its ability to
    handle changing source schemas over time. By using Iceberg's `mergeSchema`
    capability, it can unify disparate CSV files into a single, cohesive
    table. New columns are added automatically, with `null` values backfilled
    for older data.
4.  **Idempotent Writes:** It uses an 'overwrite' mode for the first batch
    and 'append' for all subsequent batches, making the job idempotent and
    safe to re-run.

Usage on EMR:
    spark-submit --deploy-mode cluster \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0 \\
        create_iceberg_s3.py \\
        --warehouse-path s3a://your-iceberg-warehouse/ \\
        --manifest-path s3a://your-manifest-bucket/manifests/source_manifest.csv \\
        --catalog-name glue_catalog \\
        --db-name your_db \\
        --table-name your_table
"""

import argparse
from pyspark.sql import SparkSession, functions as F

def parse_arguments():
    """Parses command-line arguments for the script."""
    parser = argparse.ArgumentParser(description="Create an Iceberg table on S3 from a manifest file.")
    parser.add_argument("--warehouse-path", required=True, help="S3 path for the Iceberg warehouse.")
    parser.add_argument("--manifest-path", required=True, help="S3 path to the manifest CSV file.")
    parser.add_argument("--catalog-name", default="glue_catalog", help="Name of the Iceberg catalog.")
    parser.add_argument("--db-name", required=True, help="Name of the database in the catalog.")
    parser.add_argument("--table-name", required=True, help="Name of the Iceberg table to create.")
    return parser.parse_args()

def main():
    """Main function to create and run the Spark job."""
    args = parse_arguments()
    table_identifier = f"{args.catalog_name}.{args.db_name}.{args.table_name}"

    # Standard Spark configuration for an EMR cluster environment.
    # No need for aggressive single-node memory tuning.
    spark = (
        SparkSession.builder.appName(f"Iceberg Ingestion for {args.table_name}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{args.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.catalog_name}.warehouse", args.warehouse_path)
        .config(f"spark.sql.catalog.{args.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    print("Spark session created with Iceberg support for EMR.")
    print(f"Target Iceberg table: {table_identifier}")

    # 1. Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.db_name}")
    print(f"Database '{args.db_name}' is ready.")

    # 2. Read the manifest file
    try:
        manifest_df = spark.read.option("header", "true").csv(args.manifest_path)
        print(f"Successfully read manifest file from {args.manifest_path}")
        # Group files by year and quarter to process them in logical batches
        batches = manifest_df.groupBy("year", "quarter").agg(F.collect_list("file_path").alias("files"))
        batches.persist() # Persist because we iterate over it
        print(f"Found {batches.count()} batches to process.")
    except Exception as e:
        print(f"Error reading manifest file: {args.manifest_path}")
        print(f"Please ensure the file exists and is accessible. Details: {e}")
        spark.stop()
        return

    # 3. Iteratively process each batch and write to Iceberg
    is_first_batch = True
    # Collect batches to the driver to control the loop. This is safe because the number
    # of batches (year/quarter combinations) is small.
    for row in batches.collect():
        year, quarter, files = row['year'], row['quarter'], row['files']
        write_mode = "overwrite" if is_first_batch else "append"

        print(f"\\n--- Processing Batch: Year={year}, Quarter={quarter} (Mode: {write_mode}) ---")
        print(f"Found {len(files)} files in this batch.")

        try:
            # Read all CSV files for the current batch
            # header=true and inferSchema=true are crucial for handling different file structures
            batch_df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)

            # The year and quarter are constant for the batch, so add them as columns
            batch_df = batch_df.withColumn("year", F.lit(year)).withColumn("quarter", F.lit(quarter))

            print("Writing data to Iceberg table...")
            (
                batch_df.write.format("iceberg")
                .mode(write_mode)
                .option("mergeSchema", "true")  # THIS IS THE KEY for handling schema evolution
                .option("write.spark.fanout.enabled", "true")
                .partitionBy("year", "quarter")
                .saveAsTable(table_identifier)
            )
            print("Successfully wrote batch to Iceberg.")
            is_first_batch = False

        except Exception as e:
            print(f"ERROR: Failed to process batch for Year={year}, Quarter={quarter}.")
            print(f"Full error details: {e}")
            # Optionally, you could add logic here to skip the batch and continue
            batches.unpersist()
            spark.stop()
            return

    batches.unpersist()
    print("\\nAll batches processed successfully.")

    # 4. Final verification
    print("--- Verification Step ---")
    try:
        final_df = spark.table(table_identifier)
        print(f"Final schema of table '{table_identifier}':")
        final_df.printSchema()
        print(f"Total rows in table: {final_df.count()}")
    except Exception as e:
        print(f"Could not perform final verification. Error: {e}")

    spark.stop()

if __name__ == "__main__":
    main() 