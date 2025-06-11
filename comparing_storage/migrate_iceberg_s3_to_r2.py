"""
PySpark script to migrate an Apache Iceberg table from S3 to Cloudflare R2.

This script performs a cross-cloud migration of an Iceberg table from AWS S3
to Cloudflare R2 storage. It reads from a source catalog (AWS Glue) and writes
to a destination catalog configured for R2.

Core Logic:
1.  **Dual Catalog Setup:** Configures separate catalogs for source (S3) and 
    destination (R2) to handle different storage backends.
2.  **Read Source Table:** Reads the entire source Iceberg table from the
    existing S3-based Glue catalog.
3.  **Cross-Cloud Write:** Writes the data to R2 using S3-compatible APIs
    with Cloudflare-specific endpoint configuration.
4.  **Register New Table:** Registers the migrated table in the destination
    catalog for R2.

Prerequisites:
- Cloudflare R2 bucket created
- R2 API token with read/write permissions
- Network connectivity between Spark cluster and Cloudflare R2

Usage:
    spark-submit --deploy-mode cluster \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0 \\
        migrate_iceberg_s3_to_r2.py \\
        --source-table-identifier glue_catalog.hdd_iceberg_std.hard_drive_data \\
        --dest-table-identifier r2_catalog.hdd_iceberg_r2.hard_drive_data \\
        --source-warehouse-path s3://your-iceberg-warehouse/ \\
        --dest-warehouse-path s3://your-r2-bucket/ \\
        --r2-account-id your-cloudflare-account-id \\
        --r2-access-key-id your-r2-access-key \\
        --r2-secret-access-key your-r2-secret-key
"""

import argparse
from pyspark.sql import SparkSession

def parse_arguments():
    """Parses command-line arguments for the R2 migration script."""
    parser = argparse.ArgumentParser(description="Migrate an Iceberg table from S3 to Cloudflare R2.")
    parser.add_argument("--source-table-identifier", required=True, 
                       help="Full identifier of the source table (e.g., glue_catalog.db.table).")
    parser.add_argument("--dest-table-identifier", required=True, 
                       help="Full identifier for the new R2 table (e.g., r2_catalog.db.table).")
    parser.add_argument("--source-warehouse-path", required=True, 
                       help="S3 path for the source Iceberg warehouse.")
    parser.add_argument("--dest-warehouse-path", required=True, 
                       help="R2 path for the destination warehouse (s3://bucket-name/).")
    parser.add_argument("--r2-account-id", required=True, 
                       help="Cloudflare account ID for R2 access.")
    parser.add_argument("--r2-access-key-id", required=True, 
                       help="R2 access key ID.")
    parser.add_argument("--r2-secret-access-key", required=True, 
                       help="R2 secret access key.")
    parser.add_argument("--source-catalog-name", default="glue_catalog", 
                       help="Name of the source Iceberg catalog.")
    parser.add_argument("--dest-catalog-name", default="r2_catalog", 
                       help="Name of the destination R2 catalog.")
    
    return parser.parse_args()

def main():
    """Main function to create and run the S3 to R2 migration job."""
    args = parse_arguments()

    # R2 S3-compatible endpoint
    r2_endpoint = f"https://{args.r2_account_id}.r2.cloudflarestorage.com"
    
    spark = (
        SparkSession.builder.appName(f"S3 to R2 Migration: {args.source_table_identifier}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Source catalog configuration (AWS S3 + Glue)
        .config(f"spark.sql.catalog.{args.source_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.source_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.source_catalog_name}.warehouse", args.source_warehouse_path)
        .config(f"spark.sql.catalog.{args.source_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        # Destination catalog configuration (Cloudflare R2)
        .config(f"spark.sql.catalog.{args.dest_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.warehouse", args.dest_warehouse_path)
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        # R2-specific S3 configuration
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.s3.endpoint", r2_endpoint)
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.s3.access-key-id", args.r2_access_key_id)
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.s3.secret-access-key", args.r2_secret_access_key)
        .config(f"spark.sql.catalog.{args.dest_catalog_name}.s3.path-style-access", "true")
        
        # Additional Hadoop S3A configurations for R2
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", r2_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.r2_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", args.r2_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        
        .getOrCreate()
    )

    print("Spark session created for S3 to R2 migration.")
    print(f"Source table: {args.source_table_identifier}")
    print(f"Destination table: {args.dest_table_identifier}")
    print(f"R2 endpoint: {r2_endpoint}")

    try:
        # 1. Read the entire source table from S3
        print("Reading source table from S3...")
        source_df = spark.table(args.source_table_identifier)
        source_df.persist()
        row_count = source_df.count()
        print(f"Successfully read {row_count} rows from the source table.")
        
        # Show schema for verification
        print("Source table schema:")
        source_df.printSchema()

    except Exception as e:
        print(f"ERROR: Failed to read source table '{args.source_table_identifier}'.")
        print(f"Please ensure the table exists and credentials are correct. Details: {e}")
        spark.stop()
        return

    try:
        # 2. Extract database and table names for destination
        dest_parts = args.dest_table_identifier.split('.')
        if len(dest_parts) != 3:
            raise ValueError("Destination table identifier must be in format: catalog.database.table")
        
        dest_catalog, dest_db, dest_table = dest_parts
        
        # 3. Create database in destination catalog if it doesn't exist
        print(f"Creating database '{dest_db}' in R2 catalog if needed...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_catalog}.{dest_db}")
        
        # 4. Write the DataFrame to R2 using the destination catalog
        print(f"Writing data to R2 table '{args.dest_table_identifier}'...")
        print(f"Destination warehouse path: {args.dest_warehouse_path}")
        
        (
            source_df.write.format("iceberg")
            .mode("overwrite")  # Use overwrite to make the job idempotent
            .option("path", args.dest_warehouse_path + f"{dest_db}/{dest_table}")
            .saveAsTable(args.dest_table_identifier)
        )
        
        print("Successfully migrated table to Cloudflare R2.")
        
        # 5. Verify the migration
        print("Verifying migration...")
        dest_df = spark.table(args.dest_table_identifier)
        dest_count = dest_df.count()
        print(f"Destination table contains {dest_count} rows.")
        
        if dest_count == row_count:
            print("✅ Migration verification successful - row counts match!")
        else:
            print(f"⚠️  Warning: Row count mismatch. Source: {row_count}, Destination: {dest_count}")
            
    except Exception as e:
        print("ERROR: Failed to write table to R2.")
        print(f"Full error details: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify R2 bucket exists and is accessible")
        print("2. Check R2 API credentials are correct")
        print("3. Ensure network connectivity to Cloudflare R2")
        print("4. Verify the destination warehouse path format")
    finally:
        source_df.unpersist()
        spark.stop()

    print("\nMigration job finished.")

if __name__ == "__main__":
    main() 