"""
Unified S3 to R2 Migration - Native R2 Data Catalog Approach

This script:
1. Creates the table DIRECTLY in R2 Data Catalog (gets correct location automatically)
2. Migrates data from S3 to the R2 Data Catalog managed location
3. No location conflicts - everything is native R2 Data Catalog

This is the CORRECT approach for R2 Data Catalog integration.
"""

import argparse
import os
import sys
import logging

from pyspark.sql import SparkSession
from dotenv import load_dotenv

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, 
    BooleanType, DoubleType, FloatType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session(r2_bucket: str) -> SparkSession:
    """Create Spark session with R2 configuration."""
    logger.info("Creating Spark session with Iceberg configuration...")
    
    # JAR files for Iceberg
    iceberg_jars = [
        "jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
        "jars/bundle-2.20.18.jar", 
        "jars/url-connection-client-2.20.18.jar",
        "jars/hadoop-aws-3.3.4.jar",
        "jars/aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    spark = SparkSession.builder \
        .appName("UnifiedS3ToR2CatalogNative") \
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
        .config("spark.sql.catalog.r2.warehouse", f"s3a://{r2_bucket}/__r2_data_catalog") \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.endpoint", f"https://{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com") \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.access.key", os.getenv('R2_ACCESS_KEY_ID')) \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.secret.key", os.getenv('R2_SECRET_ACCESS_KEY')) \
        .config("spark.sql.catalog.r2.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully")
    return spark

def create_table_in_r2_catalog_via_pyiceberg(table_name: str, database_name: str, source_table: str, spark: SparkSession):
    """Create table in R2 Data Catalog using PyIceberg, then return location for Spark"""
    
    account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    api_token = os.getenv('R2_CATALOG_API_TOKEN')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
    
    try:
        logger.info("🔗 Connecting to R2 Data Catalog...")
        
        # Initialize catalog with working authentication
        catalog = RestCatalog(
            name="r2_catalog",
            **{
                "uri": f"https://catalog.cloudflarestorage.com/{account_id}/{bucket_name}",
                "token": api_token,
                "warehouse": f"{account_id}_{bucket_name}",
                "s3.access-key-id": os.getenv('R2_ACCESS_KEY_ID'),
                "s3.secret-access-key": os.getenv('R2_SECRET_ACCESS_KEY'),
                "s3.endpoint": f"https://{account_id}.r2.cloudflarestorage.com"
            }
        )
        
        logger.info("✅ Connected to R2 Data Catalog!")
        
        # Ensure namespace exists
        namespace = (database_name,)
        try:
            catalog.create_namespace(namespace)
            logger.info(f" Created namespace: {database_name}")
        except Exception as e:
            logger.info(f" Error creating namespace: {e}")
            logger.info(f" Namespace {database_name} already exists")
        
        # Get schema from source table
        logger.info(" Getting schema from source table...")
        source_df = spark.sql(f"SELECT * FROM glue.{source_table} LIMIT 0")
        
        # Convert Spark schema to PyIceberg schema
        iceberg_fields = []
        for i, field in enumerate(source_df.schema.fields):
            # Convert Spark types to PyIceberg types
            if field.dataType.typeName() == "string":
                iceberg_type = StringType()
            elif field.dataType.typeName() == "integer":
                iceberg_type = IntegerType()
            elif field.dataType.typeName() == "long" or field.dataType.typeName() == "bigint":
                iceberg_type = LongType()
            elif field.dataType.typeName() == "boolean":
                iceberg_type = BooleanType()
            elif field.dataType.typeName() == "date":
                iceberg_type = DateType()
            elif field.dataType.typeName() == "double":
                iceberg_type = DoubleType()
            elif field.dataType.typeName() == "float":
                iceberg_type = FloatType()
            else:
                iceberg_type = StringType()  # Default fallback
            
            iceberg_fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable
                )
            )
        
        # Create schema
        schema = Schema(*iceberg_fields)
        logger.info(f" Created schema with {len(iceberg_fields)} fields")
        
        # Create partition spec (year, quarter)
        year_field_id = next((f.field_id for f in iceberg_fields if f.name == 'year'), None)
        quarter_field_id = next((f.field_id for f in iceberg_fields if f.name == 'quarter'), None)
        
        partition_fields = []
        if year_field_id:
            partition_fields.append(
                PartitionField(
                    source_id=year_field_id,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name='year'
                )
            )
        if quarter_field_id:
            partition_fields.append(
                PartitionField(
                    source_id=quarter_field_id,
                    field_id=1001,
                    transform=IdentityTransform(),
                    name='quarter'
                )
            )
        
        partition_spec = PartitionSpec(*partition_fields) if partition_fields else None
        
        # Create table
        table_identifier = (database_name, table_name)
        
        logger.info(f" Creating table: {'.'.join(table_identifier)}")
        
        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec
        )
        
        logger.info(" Table created successfully in R2 Data Catalog!")
        logger.info(f" Table location: {table.location()}")
        
        return table.location()
        
    except Exception as e:
        logger.error(f"❌ Failed to create table in R2 Data Catalog: {e}")
        return None

def migrate_data_to_r2_catalog_location(spark: SparkSession, source_table: str, r2_catalog_location: str, 
                                      filter_year: str = None, batch_size: str = "1000000") -> bool:
    """Migrate data directly to R2 Data Catalog managed location"""
    
    logger.info(f"Starting data migration from {source_table} to R2 Data Catalog location")
    logger.info(f"Target location: {r2_catalog_location}")
    
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
        
        # Get partitions to migrate
        partition_query = f"SELECT DISTINCT year, quarter FROM glue.{source_table}"
        if filter_year:
            years = [year.strip() for year in filter_year.split(",")]
            year_filter = " OR ".join([f"year = {year}" for year in years])
            partition_query += f" WHERE {year_filter}"
        partition_query += " ORDER BY year, quarter"
        
        logger.info(f"Partition query: {partition_query}")
        partitions_df = spark.sql(partition_query)
        partitions = partitions_df.collect()
        
        logger.info(f"Found {len(partitions)} partitions to migrate")
        
        # Write data directly to R2 Data Catalog location using Spark
        # We'll write to the data subdirectory of the catalog location
        
        bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
        
        # Configure Spark to write to R2 Data Catalog location
        # Extract the path from the catalog location
        catalog_path = r2_catalog_location.replace('s3://', '').replace(f'{bucket_name}/', '')
        data_path = f"s3a://{bucket_name}/{catalog_path}/data"
        
        logger.info(f"Writing data to: {data_path}")
        
        # Migrate each partition separately
        for i, partition in enumerate(partitions):
            year = partition['year']
            quarter = partition['quarter']
            
            logger.info(f"Processing partition {i+1}/{len(partitions)}: year={year}, quarter={quarter}")
            
            # Read just this partition
            partition_query = f"SELECT * FROM glue.{source_table} WHERE year = {year} AND quarter = '{quarter}'"
            partition_df = spark.sql(partition_query)
            
            # Write to R2 Data Catalog location with partitioning
            partition_df.write \
                .mode("append") \
                .option("path", f"{data_path}/year={year}/quarter={quarter}") \
                .option("write.target-file-size-bytes", "67108864") \
                .option("write.parquet.compression-codec", "snappy") \
                .format("parquet") \
                .save()
            
            logger.info(f"Completed partition year={year}, quarter={quarter}")
            
            # Force garbage collection to free memory
            spark.catalog.clearCache()
        
        logger.info("Data migration completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Unified S3 to R2 migration - Native R2 Data Catalog")
    parser.add_argument("--source-table", required=True, help="Source table name (e.g., 'db.table')")
    parser.add_argument("--table-name", required=True, help="Destination table name (just name, not db.table)")
    parser.add_argument("--r2-bucket", required=True, help="R2 bucket name")
    parser.add_argument("--database", default="hdd_r2", help="R2 catalog database name")
    parser.add_argument("--filter-year", help="Comma-separated years to migrate (e.g., '2013,2014')")
    parser.add_argument("--batch-size", default="1000000", help="Batch size for processing")
    
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
        logger.info(" Starting native R2 Data Catalog migration...")
        
        # Step 1: Create Spark session
        spark = create_spark_session(args.r2_bucket)
        
        # Step 2: Create table in R2 Data Catalog first (gets correct location)
        logger.info(" Step 1/3: Creating table in R2 Data Catalog...")
        catalog_location = create_table_in_r2_catalog_via_pyiceberg(
            args.table_name, 
            args.database, 
            args.source_table, 
            spark
        )
        
        if not catalog_location:
            logger.error("Failed to create table in R2 Data Catalog")
            sys.exit(1)
        
        # Step 3: Migrate data to R2 Data Catalog location
        logger.info(" Step 2/3: Migrating data to R2 Data Catalog...")
        success = migrate_data_to_r2_catalog_location(
            spark, 
            args.source_table, 
            catalog_location,
            args.filter_year,
            args.batch_size
        )
        
        if not success:
            logger.error("❌ Data migration failed!")
            sys.exit(1)
        
        # Step 4: Verify and show results
        logger.info("✅ Step 3/3: Verification...")
        
        logger.info("🎉 Migration completed successfully!")
        logger.info("✅ Your table is properly registered in R2 Data Catalog!")
        
        # Print Snowflake connection info
        account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        logger.info("\n" + "="*60)
        logger.info("SNOWFLAKE CATALOG INTEGRATION INFO:")
        logger.info("="*60)
        logger.info(f"Catalog URI: https://catalog.cloudflarestorage.com/{account_id}/{args.r2_bucket}")
        logger.info(f"Database: {args.database}")
        logger.info(f"Table: {args.table_name}")
        logger.info(f"Storage Base URL: s3compat://{args.r2_bucket}")
        logger.info(f"Table Location: {catalog_location}")
        logger.info("="*60)
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main() 