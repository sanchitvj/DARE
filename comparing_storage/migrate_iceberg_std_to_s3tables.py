"""
PySpark script to migrate an Apache Iceberg table from regular S3 storage to AWS S3 Tables.

This script migrates data from a regular Iceberg table (stored in standard S3 buckets)
to AWS S3 Tables (the new managed table storage service) while preserving table
structure, partitioning, and schema.

Core Logic:
1.  **Dual Catalog Setup:** Configures two Iceberg catalogs - one for reading from
    regular Iceberg tables and another for writing to S3 Tables.
2.  **Partition-Aware Processing:** Processes data by year+quarter partition combinations
    to handle large datasets efficiently and maintain partitioning structure.
3.  **Schema Preservation:** Uses mergeSchema to ensure compatibility and preserve
    the original table schema in the destination.
4.  **S3 Tables Integration:** Uses the S3TablesCatalog to write directly to
    AWS S3 Tables buckets.

Usage on EMR:
    spark-submit --deploy-mode client \\
        migrate_iceberg_std_to_s3tables.py \\
        --source-table-identifier glue_catalog.hdd_iceberg_std.hard_drive_data \\
        --dest-table-identifier s3tables_catalog.hdd_iceberg_s3table.hard_drive_data \\
        --source-warehouse-path s3://hdd-iceberg-std/ \\
        --dest-s3tables-bucket hdd-iceberg-s3table

Note: The destination database name typically matches the S3 Tables bucket name.
"""

import argparse
import os
import subprocess
from pyspark.sql import SparkSession

def parse_arguments():
    """Parses command-line arguments for the migration script."""
    parser = argparse.ArgumentParser(description="Migrate an Iceberg table to S3 Tables or regular S3.")
    parser.add_argument("--source-table-identifier", required=True, help="Full identifier of the source table (e.g., glue_catalog.db.table).")
    parser.add_argument("--dest-table-identifier", required=True, help="Full identifier for the new table.")
    parser.add_argument("--source-warehouse-path", required=True, help="S3 path for the source Iceberg warehouse.")
    parser.add_argument("--dest-s3tables-bucket", help="Name of the S3 Tables bucket (without s3:// prefix). If not provided, will use regular S3.")
    parser.add_argument("--dest-s3-path", help="S3 path for regular S3 destination (e.g., s3://bucket/path/). Used when S3 Tables not available.")
    parser.add_argument("--source-catalog-name", default="glue_catalog", help="Name of the source Iceberg catalog.")
    parser.add_argument("--dest-catalog-name", default="s3tables_catalog", help="Name of the destination catalog.")
    parser.add_argument("--aws-region", default="us-east-1", help="AWS region for S3 Tables ARN.")
    parser.add_argument("--batch-size", type=int, default=1000000, help="Number of rows to process per batch.")
    parser.add_argument("--use-s3tables", action="store_true", help="Force S3 Tables mode (will fail if not supported).")

    return parser.parse_args()

def migrate_in_batches(spark, args, source_df):
    """Migrate data in batches to handle large datasets more reliably."""
    print("Starting batch migration process...")
    
    # Determine migration mode
    use_s3tables = args.use_s3tables or args.dest_s3tables_bucket
    migration_mode = "S3 Tables" if use_s3tables else "Regular S3"
    print(f"Migration mode: {migration_mode}")
    
    # First check the source table schema and row count
    print("Source table schema:")
    source_df.printSchema()
    total_rows = source_df.count()
    print(f"Source table contains {total_rows} total rows")
    
    if total_rows == 0:
        print("ERROR: Source table is empty. Nothing to migrate.")
        return
    
    # Check if destination table exists
    try:
        spark.sql(f"DESCRIBE TABLE {args.dest_table_identifier}")
        table_exists = True
        print(f"Destination table {args.dest_table_identifier} already exists. Will append new data.")
    except Exception as e:
        print(f"Error checking table existence: {e}")
        table_exists = False
        print(f"Destination table {args.dest_table_identifier} does not exist. Will create it.")
    
    # Get distinct partition combinations (year, quarter)
    if "year" in source_df.columns and "quarter" in source_df.columns:
        print("Found both 'year' and 'quarter' partition columns in source table")
        partition_combinations = source_df.select("year", "quarter").distinct().collect()
        print(f"Found {len(partition_combinations)} partition combinations to process:")
        for row in partition_combinations:
            print(f"  - year={row.year}, quarter={row.quarter}")
        
        if len(partition_combinations) == 0:
            print("ERROR: No partition combinations found. This shouldn't happen.")
            return
        
        for i, row in enumerate(partition_combinations):
            year, quarter = row.year, row.quarter
            print(f"\\n--- Processing partition year={year}, quarter={quarter} ({i+1}/{len(partition_combinations)}) ---")
            
            # Filter by both year and quarter
            partition_df = source_df.filter((source_df.year == year) & (source_df.quarter == quarter))
            row_count = partition_df.count()
            print(f"Partition contains {row_count} rows")
            
            if row_count == 0:
                print("Skipping empty partition")
                continue
            
            # Use overwrite for first partition if table doesn't exist, otherwise append
            mode = "overwrite" if (not table_exists and i == 0) else "append"
            print(f"Writing partition to {migration_mode} in '{mode}' mode...")
            
            try:
                writer = partition_df.write.format("iceberg").mode(mode).option("mergeSchema", "true")
                
                # Add partitioning for the first write (table creation)
                if not table_exists and i == 0:
                    writer = writer.partitionBy("year", "quarter")
                
                # Add path option for regular S3 mode
                if not use_s3tables and args.dest_s3_path:
                    writer = writer.option("path", args.dest_s3_path)
                
                writer.saveAsTable(args.dest_table_identifier)
                print(f"Successfully migrated partition year={year}, quarter={quarter} to {migration_mode}")
                
                # Mark table as existing after first successful write
                if not table_exists:
                    table_exists = True
                    
            except Exception as e:
                print(f"ERROR: Failed to migrate partition year={year}, quarter={quarter} to {migration_mode}")
                print(f"Error details: {e}")
                
                # If S3 Tables failed, suggest fallback
                if use_s3tables and "S3TablesCatalog" in str(e):
                    print("\\n*** S3 Tables not supported in this EMR version ***")
                    print("Consider using regular S3 migration instead:")
                    print("  --dest-s3-path s3://your-bucket/warehouse/")
                    print("  --dest-catalog-name glue_catalog")
                
                raise e
    
    else:
        print(f"Partition columns 'year' and/or 'quarter' not found in source table columns: {source_df.columns}")
        print("Processing entire dataset at once...")
        mode = "overwrite" if not table_exists else "append"
        print(f"Writing entire table to {migration_mode} in '{mode}' mode...")
        
        try:
            writer = source_df.write.format("iceberg").mode(mode).option("mergeSchema", "true")
            
            # Try to preserve partitioning if columns exist
            if "year" in source_df.columns and "quarter" in source_df.columns:
                writer = writer.partitionBy("year", "quarter")
            elif "year" in source_df.columns:
                writer = writer.partitionBy("year")
                
            # Add path option for regular S3 mode
            if not use_s3tables and args.dest_s3_path:
                writer = writer.option("path", args.dest_s3_path)
            
            writer.saveAsTable(args.dest_table_identifier)
            print("Successfully migrated entire table to {migration_mode}")
        except Exception as e:
            print("ERROR: Failed to migrate table to {migration_mode}")
            print(f"Error details: {e}")
            raise e

def get_aws_account_id():
    """Get AWS account ID from environment or AWS CLI."""
    # Try environment variable first
    account_id = os.environ.get('AWS_ACCOUNT_ID')
    if account_id:
        return account_id
    
    # Try AWS CLI get-caller-identity
    try:
        result = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception as e:
        print(f"Warning: Could not auto-detect AWS account ID via AWS CLI: {e}")
    
    # Fallback: try to extract from IAM role ARN in environment
    try:
        aws_role_arn = os.environ.get('AWS_ROLE_ARN') or os.environ.get('AWS_EXECUTION_ENV')
        if aws_role_arn and 'arn:aws:iam::' in aws_role_arn:
            # Extract account ID from ARN format: arn:aws:iam::123456789012:role/...
            account_id = aws_role_arn.split(':')[4]
            if account_id.isdigit() and len(account_id) == 12:
                return account_id
    except Exception:
        pass
    
    print("Warning: Could not auto-detect AWS account ID")
    return None

def main():
    """Main function to create and run the Spark migration job."""
    args = parse_arguments()

    # Get AWS account ID
    aws_account_id = get_aws_account_id()
    if not aws_account_id:
        print("ERROR: Could not determine AWS account ID. Set AWS_ACCOUNT_ID environment variable.")
        return

    print(f"Using AWS Account ID: {aws_account_id}")

    # Determine if we should use S3 Tables or regular S3
    use_s3tables = args.use_s3tables or args.dest_s3tables_bucket
    
    spark_builder = (
        SparkSession.builder.appName(f"Iceberg to S3 Tables Migration: {args.source_table_identifier}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Configure source catalog (regular Iceberg)
        .config(f"spark.sql.catalog.{args.source_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.source_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.source_catalog_name}.warehouse", args.source_warehouse_path)
        .config(f"spark.sql.catalog.{args.source_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        # Add configurations for better stability
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.iceberg.vectorization.enabled", "false")  # More stable for large migrations
    )
    
    # Configure destination catalog based on mode
    if use_s3tables and args.dest_s3tables_bucket and aws_account_id:
        # S3 Tables mode
        spark_builder = (spark_builder
            .config(f"spark.sql.catalog.{args.dest_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{args.dest_catalog_name}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
            .config(f"spark.sql.catalog.{args.dest_catalog_name}.warehouse", f"arn:aws:s3tables:{args.aws_region}:{aws_account_id}:bucket/{args.dest_s3tables_bucket}")
        )
        print("Configuring for S3 Tables mode")
    # else:
    #     # Regular S3 mode (fallback to Glue catalog)
    #     spark_builder = (spark_builder
    #         .config(f"spark.sql.catalog.{args.dest_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    #         .config(f"spark.sql.catalog.{args.dest_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    #         .config(f"spark.sql.catalog.{args.dest_catalog_name}.warehouse", args.dest_s3_path or f"s3://{args.dest_s3tables_bucket}/warehouse/")
    #         .config(f"spark.sql.catalog.{args.dest_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    #     )
    #     print("Configuring for regular S3 mode (using Glue catalog)")
    
    spark = spark_builder.getOrCreate()

    print("Spark session created with dual catalogs (Iceberg + S3 Tables).")
    print(f"Source: {args.source_table_identifier}")
    print(f"Destination: {args.dest_table_identifier}")

    try:
        # Extract database name from destination table identifier  
        dest_parts = args.dest_table_identifier.split('.')
        if len(dest_parts) >= 3:
            dest_catalog, dest_database, _ = dest_parts[0], dest_parts[1], dest_parts[2]
            
            # Ensure the destination database exists in S3 Tables catalog
            try:
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_catalog}.{dest_database}")
                print(f"Ensured database '{dest_database}' exists in S3 Tables catalog")
            except Exception as e:
                print(f"Warning: Could not create/verify database {dest_database}: {e}")
        
        # 1. Read from source Iceberg table
        source_df = spark.table(args.source_table_identifier)
        print("Successfully connected to source Iceberg table")

        # 2. Migrate to S3 Tables using batch processing
        migrate_in_batches(spark, args, source_df)
        print("Successfully migrated table from Iceberg to S3 Tables using batch processing.")
        
    except Exception as e:
        print(f"ERROR: Failed to migrate table '{args.source_table_identifier}' to S3 Tables.")
        print(f"Please ensure both catalogs are properly configured and accessible. Details: {e}")
        raise e  # Re-raise to ensure the job fails properly
    finally:
        spark.stop()

    print("\nMigration job finished.")

if __name__ == "__main__":
    main() 