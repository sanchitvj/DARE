"""
Pure Spark Iceberg Migration: S3 to R2

This script uses Spark to read Iceberg data from S3 (Glue catalog) and write it
directly to R2 (R2 Data Catalog) without loading data into local memory.

This is simpler than rclone + add_files and handles the entire migration in one step.
instance used: c6a.4xlarge
"""

import argparse
import os
import sys
import logging

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session(r2_bucket: str) -> SparkSession:
    """Create Spark session with both Glue and R2 catalogs configured."""
    logger.info("Creating Spark session with Iceberg configuration...")
    
    # Debug: Check if environment variables are loaded
    logger.info(f"R2_ACCESS_KEY_ID: {'SET' if os.getenv('R2_ACCESS_KEY_ID') else 'NOT SET'}")
    logger.info(f"R2_SECRET_ACCESS_KEY: {'SET' if os.getenv('R2_SECRET_ACCESS_KEY') else 'NOT SET'}")
    logger.info(f"CLOUDFLARE_ACCOUNT_ID: {'SET' if os.getenv('CLOUDFLARE_ACCOUNT_ID') else 'NOT SET'}")
    logger.info(f"R2_CATALOG_API_TOKEN: {'SET' if os.getenv('R2_CATALOG_API_TOKEN') else 'NOT SET'}")
    
    # JAR files for Iceberg
    iceberg_jars = [
        "jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
        "jars/bundle-2.20.18.jar", 
        "jars/url-connection-client-2.20.18.jar",
        "jars/hadoop-aws-3.3.4.jar",
        "jars/aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    spark = SparkSession.builder \
        .appName("IcebergS3ToR2Migration") \
        .config("spark.jars", ",".join(iceberg_jars)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.memory", "20g") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "128MB") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "50") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.local.dir", "/mnt/tmp,/tmp") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.io.compression.codec", "snappy") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.type", "glue") \
        .config("spark.sql.catalog.glue.warehouse", "s3://your-glue-warehouse/") \
        .config("spark.sql.catalog.r2", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.r2.type", "hadoop") \
        .config("spark.sql.catalog.r2.warehouse", f"s3a://{r2_bucket}/iceberg") \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.endpoint", f"https://{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com") \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.access.key", os.getenv('R2_ACCESS_KEY_ID')) \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.secret.key", os.getenv('R2_SECRET_ACCESS_KEY')) \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully")
    return spark

def create_destination_table(spark: SparkSession, source_table: str, dest_table: str, r2_bucket: str) -> bool:
    """Create destination table with same schema and partitioning as source."""
    logger.info(f"Creating destination table: {dest_table}")
    
    try:
        # Get the actual schema from source table (handles evolving schemas)
        logger.info("Getting schema from source table...")
        source_df = spark.sql(f"SELECT * FROM glue.{source_table} LIMIT 0")
        
        # Build CREATE TABLE statement with actual schema
        columns = []
        for field in source_df.schema.fields:
            # Convert Spark types to Iceberg/SQL types
            if field.dataType.typeName() == "string":
                col_type = "STRING"
            elif field.dataType.typeName() == "integer":
                col_type = "INT"
            elif field.dataType.typeName() == "long" or field.dataType.typeName() == "bigint":
                col_type = "BIGINT"
            elif field.dataType.typeName() == "boolean":
                col_type = "BOOLEAN"
            elif field.dataType.typeName() == "date":
                col_type = "DATE"
            elif field.dataType.typeName() == "double":
                col_type = "DOUBLE"
            elif field.dataType.typeName() == "float":
                col_type = "FLOAT"
            else:
                col_type = "STRING"  # Default fallback
                
            columns.append(f"`{field.name}` {col_type}")
        
        columns_sql = ",\n            ".join(columns)
        
        create_table_sql = f"""
        CREATE TABLE r2.{dest_table} (
            {columns_sql}
        )
        USING ICEBERG
        PARTITIONED BY (year, quarter)
        LOCATION 's3a://{r2_bucket}/iceberg/{dest_table.replace(".", "/")}'
        """
        
        logger.info(f"Creating table with {len(columns)} columns...")
        spark.sql(create_table_sql)
        logger.info(f"Successfully created destination table: {dest_table}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create destination table: {e}")
        return False

def migrate_data(spark: SparkSession, source_table: str, dest_table: str, 
                filter_year: str = None, batch_size: str = "1000000") -> bool:
    """Migrate data from source to destination table in batches."""
    logger.info(f"Starting data migration from {source_table} to {dest_table}")
    
    try:
        # Build the source query with optional year filter
        base_query = f"SELECT * FROM glue.{source_table}"
        
        if filter_year:
            years = [year.strip() for year in filter_year.split(",")]
            year_filter = " OR ".join([f"year = {year}" for year in years])
            base_query += f" WHERE {year_filter}"
        
        logger.info(f"Source query: {base_query}")
        
        # Read source data
        logger.info("Reading source data...")
        source_df = spark.sql(base_query)
        
        # Show some stats
        logger.info("Analyzing source data...")
        total_count = source_df.count()
        logger.info(f"Total records to migrate: {total_count:,}")
        
        if total_count == 0:
            logger.warning("No data found to migrate")
            return False
        
        # Process in batches by partition to reduce memory pressure
        logger.info("Getting unique partitions...")
        partitions_df = spark.sql(f"SELECT DISTINCT year, quarter FROM glue.{source_table} ORDER BY year, quarter")
        partitions = partitions_df.collect()
        
        logger.info(f"Found {len(partitions)} partitions to migrate")
        
        # Migrate each partition separately
        for i, partition in enumerate(partitions):
            year = partition['year']
            quarter = partition['quarter']
            
            logger.info(f"Processing partition {i+1}/{len(partitions)}: year={year}, quarter={quarter}")
            
            # Read just this partition - handle quarter as string
            partition_query = f"SELECT * FROM glue.{source_table} WHERE year = {year} AND quarter = '{quarter}'"
            partition_df = spark.sql(partition_query)
            
            # Write to destination
            partition_df.write \
                .mode("append") \
                .option("write.target-file-size-bytes", "67108864") \
                .option("write.parquet.compression-codec", "snappy") \
                .saveAsTable(f"r2.{dest_table}")
            
            logger.info(f"Completed partition year={year}, quarter={quarter}")
            
            # Force garbage collection to free memory
            spark.catalog.clearCache()
        
        logger.info("Data migration completed successfully!")
        
        # Verify the migration
        logger.info("Verifying migration...")
        dest_count = spark.sql(f"SELECT COUNT(*) as count FROM r2.{dest_table}").collect()[0]['count']
        logger.info(f"Destination table record count: {dest_count:,}")
        
        if dest_count > 0:
            logger.info("Migration verification successful!")
            return True
        else:
            logger.error("Migration verification failed - no records found in destination")
            return False
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Migrate Iceberg table from S3 to R2 using Spark")
    parser.add_argument("--source-table", required=True, help="Source table name (e.g., 'db.table')")
    parser.add_argument("--dest-table", required=True, help="Destination table name (e.g., 'db.table')")
    parser.add_argument("--r2-bucket", required=True, help="R2 bucket name")
    parser.add_argument("--filter-year", help="Comma-separated years to migrate (e.g., '2013,2014')")
    parser.add_argument("--batch-size", default="1000000", help="Batch size for processing")
    parser.add_argument("--drop-existing", action="store_true", help="Drop existing destination table")
    
    args = parser.parse_args()
    
    # Check required environment variables
    required_vars = [
        'R2_ACCESS_KEY_ID',
        'R2_SECRET_ACCESS_KEY', 
        'CLOUDFLARE_ACCOUNT_ID',
        'R2_CATALOG_API_TOKEN'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {missing_vars}")
        sys.exit(1)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session(args.r2_bucket)
        
        # Drop existing table if requested
        if args.drop_existing:
            logger.info(f"Dropping existing table: {args.dest_table}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS r2.{args.dest_table}")
                logger.info("Existing table dropped")
            except Exception as e:
                logger.warning(f"Could not drop existing table: {e}")
        
        # Create destination table
        if not create_destination_table(spark, args.source_table, args.dest_table, args.r2_bucket):
            logger.error("Failed to create destination table")
            sys.exit(1)
        
        # Migrate data
        success = migrate_data(
            spark, 
            args.source_table, 
            args.dest_table, 
            args.filter_year,
            args.batch_size
        )
        
        if success:
            logger.info("🎉 Migration completed successfully!")
            
            # Show final stats
            logger.info("Final verification:")
            spark.sql(f"SELECT year, quarter, COUNT(*) as records FROM r2.{args.dest_table} GROUP BY year, quarter ORDER BY year, quarter").show()
            
            sys.exit(0)
        else:
            logger.error("❌ Migration failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main() 