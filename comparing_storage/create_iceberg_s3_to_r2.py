"""
High-Performance Iceberg-to-Iceberg Migration: S3 to R2 Data Catalog
==================================================================

This script migrates existing Iceberg tables from S3 to Cloudflare R2 Data Catalog.
Much more efficient than CSV-based migration due to:
- Columnar Parquet format (5-10x smaller than CSV)
- Built-in compression and optimization
- Schema already defined (no inference needed)
- Predicate pushdown and column pruning

Features:
- Dual catalog configuration (S3 source + R2 destination)
- Batch processing with memory optimization
- Schema evolution handling
- Partition preservation or re-partitioning
- Table optimization after migration

Prerequisites:
1. Run download_iceberg_jars.sh in the r2 directory first
2. Set up environment variables in .env file
3. Install dependencies: pip install pyspark python-dotenv psutil

Usage:
    python create_iceberg_s3_to_r2.py --source-table glue_catalog.database.table --dest-table r2_table
"""

import os
import sys
import gc
import argparse
import psutil
import multiprocessing
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when
from dotenv import load_dotenv

load_dotenv()

def create_spark_session_dual_catalog(app_name="S3-to-R2-Iceberg-Migration"):
    """
    Create Spark session with dual catalog configuration:
    - s3_catalog: Source Iceberg tables in S3
    - r2_catalog: Destination R2 Data Catalog
    """
    cpu_cores = multiprocessing.cpu_count()
    spark_cores = max(2, cpu_cores - 2)
    
    print(f"Detected {cpu_cores} CPU cores, using {spark_cores} for Spark")
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    r2_dir = os.path.join(script_dir, "r2")
    jars_dir = os.path.join(r2_dir, "jars")
    
    # Check if JARs directory exists
    if not os.path.exists(jars_dir):
        print(f"JARs directory not found: {jars_dir}")
        print("Please run the download_iceberg_jars.sh script first!")
        sys.exit(1)
    
    # Build list of required JAR files
    required_jars = [
        "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
        "bundle-2.20.18.jar", 
        "url-connection-client-2.20.18.jar",
        "hadoop-aws-3.3.4.jar",
        "aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    jar_paths = []
    for jar in required_jars:
        jar_path = os.path.join(jars_dir, jar)
        if os.path.exists(jar_path):
            jar_paths.append(jar_path)
        else:
            print(f"Missing JAR: {jar_path}")
            print("Please run the download_iceberg_jars.sh script!")
            sys.exit(1)
    
    # Join all JAR paths
    jars_string = ",".join(jar_paths)
    print(f"Using {len(jar_paths)} JAR files")
    
    # Automatically detect and allocate memory
    try:
        available_memory_gb = psutil.virtual_memory().available // (1024**3)
        driver_memory_gb = max(4, min(28, int(available_memory_gb * 0.8)))
        print(f"Auto-detected memory: {available_memory_gb}GB available, using {driver_memory_gb}GB for Spark")
    except ImportError:
        driver_memory_gb = 8
        print(f"Using default memory allocation: {driver_memory_gb}GB")
    
    # AWS region for Glue
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    
    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(f"local[{spark_cores}]") \
        .config("spark.jars", jars_string) \
        .config("spark.driver.memory", f"{driver_memory_gb}g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "209715200") \
        .config("spark.hadoop.fs.s3a.threads.max", "32") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", os.getenv("S3_WAREHOUSE")) \
        .config("spark.sql.catalog.glue_catalog.s3.access-key-id", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.sql.catalog.glue_catalog.s3.secret-access-key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.sql.catalog.glue_catalog.glue.region", aws_region) \
        .config("spark.sql.catalog.r2_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.r2_catalog.type", "rest") \
        .config("spark.sql.catalog.r2_catalog.uri", os.getenv("R2_CATALOG_URI")) \
        .config("spark.sql.catalog.r2_catalog.warehouse", os.getenv("R2_WAREHOUSE")) \
        .config("spark.sql.catalog.r2_catalog.token", os.getenv("R2_CATALOG_API_TOKEN")) \
        .config("spark.sql.catalog.r2_catalog.oauth2-server-uri", f"{os.getenv('R2_CATALOG_URI')}/oauth/tokens") \
        .config("spark.sql.catalog.r2_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.r2_catalog.s3.endpoint", os.getenv("R2_ENDPOINT")) \
        .config("spark.sql.catalog.r2_catalog.s3.access-key-id", os.getenv("R2_ACCESS_KEY_ID")) \
        .config("spark.sql.catalog.r2_catalog.s3.secret-access-key", os.getenv("R2_SECRET_ACCESS_KEY"))
    
    # Optional Glue catalog ID
    glue_catalog_id = os.getenv("AWS_GLUE_CATALOG_ID")
    if glue_catalog_id:
        builder = builder.config("spark.sql.catalog.glue_catalog.glue.catalog-id", glue_catalog_id)
    
    print(f"✅ Glue catalog configured for region: {aws_region}")
    return builder.getOrCreate()



def get_table_info(spark, source_table):
    """Get information about the source Iceberg table"""
    print(f"Analyzing source table: {source_table}")
    
    df = spark.table(source_table)
    total_rows = df.count()
    has_year_column = 'year' in [field.name for field in df.schema.fields]
    
    print(f"Total rows: {total_rows:,}, Has year column: {has_year_column}")
    
    return {
        'total_rows': total_rows,
        'has_year_column': has_year_column
    }

def migrate_table_batch_by_year(spark, source_table, dest_table, table_info, spark_cores, years=None, drop_existing=False):
    """
    Migrate Iceberg table by processing year-by-year batches for memory efficiency
    """
    print(f"\n=== Starting Migration: {source_table} → r2_catalog.default.{dest_table} ===")
    
    # Drop existing destination table if requested
    if drop_existing:
        try:
            spark.sql(f"DROP TABLE IF EXISTS r2_catalog.default.{dest_table}")
            print(f"✅ Dropped existing table: {dest_table}")
        except Exception as e:
            print(f"Note: Could not drop table (may not exist): {e}")
    
    # Get available years from source table
    if years is None:
        if table_info['has_year_column']:
            print("Auto-detecting available years...")
            years_df = spark.sql(f"SELECT DISTINCT year FROM {source_table} ORDER BY year")
            years = [row.year for row in years_df.collect()]
            print(f"Found years: {years}")
        else:
            print("No year column found - processing entire table as single batch")
            years = [None]  # Single batch
    
    total_rows_migrated = 0
    
    # Process each year
    for i, year in enumerate(years):
        if year is not None:
            print(f"\n--- Processing Year {year} ({i+1}/{len(years)}) ---")
            
            # Read data for specific year with predicate pushdown
            df = spark.sql(f"SELECT * FROM {source_table} WHERE year = {year}")
        else:
            print("\n--- Processing Entire Table ---")
            df = spark.table(source_table)
        
        # Get row count for this batch
        batch_rows = df.count()
        print(f"Batch size: {batch_rows:,} rows")
        
        if batch_rows == 0:
            print("No data for this batch, skipping...")
            continue
        
        # Add quarter partitioning column if year column exists
        if table_info['has_year_column']:
            df = df.withColumn("quarter", 
                when(month(col("date")).between(1, 3), "Q1")
                .when(month(col("date")).between(4, 6), "Q2") 
                .when(month(col("date")).between(7, 9), "Q3")
                .when(month(col("date")).between(10, 12), "Q4")
                .otherwise("Q1"))
        
        # Write to R2 catalog
        if i == 0:  # First batch - create table
            print(f"Creating destination table: r2_catalog.default.{dest_table}")
            
            if table_info['has_year_column']:
                # Create partitioned table
                df.writeTo(f"r2_catalog.default.{dest_table}") \
                    .partitionedBy("year", "quarter") \
                    .tableProperty("format-version", "2") \
                    .tableProperty("write.target-file-size-bytes", "268435456") \
                    .tableProperty("write.parquet.compression-codec", "snappy") \
                    .tableProperty("write.parquet.row-group-size-bytes", "67108864") \
                    .tableProperty("write.merge-schema", "true") \
                    .createOrReplace()
            else:
                # Create non-partitioned table
                df.writeTo(f"r2_catalog.default.{dest_table}") \
                    .tableProperty("format-version", "2") \
                    .tableProperty("write.target-file-size-bytes", "268435456") \
                    .tableProperty("write.parquet.compression-codec", "snappy") \
                    .tableProperty("write.parquet.row-group-size-bytes", "67108864") \
                    .tableProperty("write.merge-schema", "true") \
                    .createOrReplace()
        else:  # Subsequent batches - append
            df.writeTo(f"r2_catalog.default.{dest_table}") \
                .option("mergeSchema", "true") \
                .append()
        
        total_rows_migrated += batch_rows
        print(f"✅ Batch completed: {batch_rows:,} rows written")
        
        # Memory cleanup
        df.unpersist()
        del df
        gc.collect()
    
    print(f"\n✅ Migration completed: {total_rows_migrated:,} total rows migrated")
    return total_rows_migrated



def optimize_table(spark, table_name):
    """Run table optimization for better query performance"""
    print(f"Optimizing table: {table_name}")
    
    try:
        spark.sql(f"CALL r2_catalog.system.rewrite_data_files(table => 'default.{table_name}')")
        spark.sql(f"CALL r2_catalog.system.rewrite_manifests('default.{table_name}')")
        print("✅ Table optimization completed")
    except Exception as e:
        print(f"⚠️ Table optimization failed: {e}")

def verify_migration(spark, source_table, dest_table):
    """Verify the migration was successful"""
    print("\n=== Verifying Migration ===")
    
    source_count = spark.table(source_table).count()
    dest_count = spark.table(f"r2_catalog.default.{dest_table}").count()
    
    print(f"Source: {source_count:,} rows, Destination: {dest_count:,} rows")
    
    if source_count == dest_count:
        print("✅ Migration successful!")
        return True
    else:
        print("⚠️ Row count mismatch!")
        return False

def main():
    parser = argparse.ArgumentParser(description="Migrate Iceberg tables from S3 to R2 Data Catalog")
    parser.add_argument("--source-table", required=True, 
                       help="Source table name (e.g., s3_catalog.database.table_name)")
    parser.add_argument("--dest-table", required=True,
                       help="Destination table name in R2 catalog")
    parser.add_argument("--years", nargs="+", type=int, default=None,
                       help="Specific years to migrate (e.g., --years 2013 2014). If not specified, migrates all data.")

    parser.add_argument("--drop-existing", action="store_true",
                       help="Drop existing destination table before migration")
    parser.add_argument("--optimize-table", action="store_true",
                       help="Run table optimization after migration")
    parser.add_argument("--verify", action="store_true",
                       help="Verify migration by comparing row counts")
    parser.add_argument("--verbose", action="store_true",
                       help="Verbose output")
    parser.add_argument("--batch-size", type=int, default=1,
                       help="Number of years to process in parallel (default: 1)")
    args = parser.parse_args()
    
    # Validate environment variables
    required_env_vars = [
        "R2_CATALOG_URI", "R2_WAREHOUSE", "R2_CATALOG_API_TOKEN", 
        "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
        "S3_WAREHOUSE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
    ]
    
    # Validate AWS_REGION is provided
    if not os.getenv("AWS_REGION"):
        print("❌ AWS_REGION is required")
        sys.exit(1)
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    print("Starting Iceberg-to-Iceberg Migration (S3 → R2)")
    print(f"Source: {args.source_table}")
    print(f"Destination: r2_catalog.default.{args.dest_table}")
    
    # Record start time
    start_time = time.time()
    
    # Create Spark session with dual catalog configuration
    spark = create_spark_session_dual_catalog()
    
    # Suppress verbose logs
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Analyze source table
        table_info = get_table_info(spark, args.source_table)
        
        # Migrate table using batch-by-year strategy
        cpu_cores = multiprocessing.cpu_count()
        spark_cores = max(2, cpu_cores - 2)
        total_migrated = migrate_table_batch_by_year(
            spark, args.source_table, args.dest_table, table_info, spark_cores,
            years=args.years, drop_existing=args.drop_existing
        )
        
        # Optimize table if requested
        if args.optimize_table:
            optimize_table(spark, args.dest_table)
        
        # Verify migration if requested
        if args.verify:
            verify_migration(spark, args.source_table, args.dest_table)
        
        # Calculate total execution time
        end_time = time.time()
        total_duration = end_time - start_time
        
        print("\n=== Migration Summary ===")
        print(f"Total rows migrated: {total_migrated:,}")
        print(f"Total time: {total_duration:.1f}s")
        print(f"Migration rate: {total_migrated / total_duration:.0f} rows/second")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 