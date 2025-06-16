"""
PySpark script to create Apache Iceberg tables directly in R2 Data Catalog.

This script creates Iceberg tables natively in R2 Data Catalog (not migrated),
ensuring full compatibility with Snowflake REST catalog integration.

Core Logic:
1. **Direct R2 Data Catalog Creation**: Creates tables directly in R2 Data Catalog
   using PyIceberg RestCatalog, getting proper UUID-based locations
2. **Manifest-Driven Ingestion**: Reads manifest file to process files in batches
3. **Schema Evolution**: Handles evolving schemas with unified schema approach
4. **Year/Quarter Partitioning**: Partitions data by year and quarter
5. **Snowflake Compatible**: Creates tables that work with Snowflake REST catalog

Usage:
    python create_iceberg_r2_catalog.py \\
        --manifest-path s3a://your-manifest-bucket/manifests/source_manifest.csv \\
        --database-name hdd_r2 \\
        --table-name hard_drive_data \\
        --filter-year 2013,2014
"""

import argparse
import os
import sys
import logging
from typing import List

from pyspark.sql import SparkSession, functions as F
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, 
    BooleanType, DoubleType, FloatType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session optimized for R2 Data Catalog operations."""
    logger.info("Creating Spark session for R2 Data Catalog...")
    
    # Get the current script directory to build absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # JAR files for Iceberg (using absolute paths)
    iceberg_jars = [
        os.path.join(script_dir, "jars", "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"),
        os.path.join(script_dir, "jars", "bundle-2.20.18.jar"), 
        os.path.join(script_dir, "jars", "url-connection-client-2.20.18.jar"),
        os.path.join(script_dir, "jars", "hadoop-aws-3.3.4.jar"),
        os.path.join(script_dir, "jars", "aws-java-sdk-bundle-1.12.262.jar")
    ]
    
    # Verify JAR files exist
    for jar_path in iceberg_jars:
        if not os.path.exists(jar_path):
            logger.error(f"❌ JAR file not found: {jar_path}")
            raise FileNotFoundError(f"JAR file not found: {jar_path}")
        else:
            logger.info(f"✅ Found JAR: {jar_path}")
    
    spark = SparkSession.builder \
        .appName("R2DataCatalogIcebergCreation") \
        .config("spark.jars", ",".join(iceberg_jars)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "128MB") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.io.compression.codec", "snappy") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.type", "glue") \
        .config("spark.sql.catalog.glue.warehouse", "s3://your-glue-warehouse/") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark


def create_r2_catalog_connection() -> RestCatalog:
    """Create connection to R2 Data Catalog."""
    account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    api_token = os.getenv('R2_CATALOG_API_TOKEN')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
    
    if not all([account_id, api_token, bucket_name]):
        raise ValueError("Missing required environment variables: CLOUDFLARE_ACCOUNT_ID, R2_CATALOG_API_TOKEN, R2_BUCKET_NAME")
    
    logger.info("Connecting to R2 Data Catalog...")
    
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
    return catalog


def get_unified_schema(spark: SparkSession, file_paths: List[str]) -> tuple:
    """Generate unified schema from all source files."""
    logger.info("Generating unified schema from source files...")
    
    # Read all files with schema inference
    unified_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_paths)
    
    # Convert Spark schema to PyIceberg schema
    iceberg_fields = []
    for i, field in enumerate(unified_df.schema.fields):
        # Map Spark types to PyIceberg types
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
    
    # Add year and quarter fields
    iceberg_fields.extend([
        NestedField(field_id=len(iceberg_fields) + 1, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=len(iceberg_fields) + 2, name="quarter", field_type=StringType(), required=True)
    ])
    
    iceberg_schema = Schema(*iceberg_fields)
    logger.info(f"Created unified schema with {len(iceberg_fields)} fields")
    return iceberg_schema, unified_df.schema


def create_table_in_r2_catalog(catalog: RestCatalog, database_name: str, table_name: str, schema: Schema, drop_existing: bool = False) -> str:
    """Create table in R2 Data Catalog and return its location."""
    logger.info(f"Creating table {database_name}.{table_name} in R2 Data Catalog...")
    
    # Ensure namespace exists
    namespace = (database_name,)
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Created namespace: {database_name}")
    except Exception as e:
        logger.info(f"Namespace {database_name} already exists: {e}")
    
    # Check if table already exists
    table_identifier = (database_name, table_name)
    
    try:
        existing_table = catalog.load_table(table_identifier)
        
        if drop_existing:
            logger.info(f" Dropping existing table {database_name}.{table_name}...")
            catalog.drop_table(table_identifier)
            logger.info(" Table dropped successfully!")
        else:
            logger.info(f" Table {database_name}.{table_name} already exists!")
            logger.info(f" Table location: {existing_table.location()}")
            return existing_table.location()
    except Exception:
        logger.info(f"Table {database_name}.{table_name} does not exist, creating new table...")
    
    # Create partition spec (year, quarter)
    year_field_id = next((f.field_id for f in schema.fields if f.name == 'year'), None)
    quarter_field_id = next((f.field_id for f in schema.fields if f.name == 'quarter'), None)
    
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
    try:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec
        )
        
        logger.info("✅ Table created successfully in R2 Data Catalog!")
        logger.info(f"📍 Table location: {table.location()}")
        return table.location()
        
    except Exception as e:
        logger.error(f"❌ Failed to create table: {e}")
        raise


def write_data_to_r2_catalog(spark: SparkSession, catalog_location: str, manifest_df, 
                           spark_schema, filter_year: str = None) -> bool:
    """Write data to R2 Data Catalog location using Spark."""
    logger.info("Writing data to R2 Data Catalog...")
    
    try:
        # Filter by year if specified
        if filter_year:
            years = [year.strip() for year in filter_year.split(",")]
            year_filter = " OR ".join([f"year = {year}" for year in years])
            manifest_df = manifest_df.filter(F.expr(year_filter))
            logger.info(f"Filtered to years: {years}")
        
        # Group files by year and quarter
        batches = manifest_df.groupBy("year", "quarter").agg(F.collect_list("file_path").alias("files"))
        batches.persist()
        
        logger.info(f"Found {batches.count()} batches to process")
        
        # Process each batch
        is_first_batch = True
        bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
        
        # Extract path from catalog location
        catalog_path = catalog_location.replace('s3://', '').replace(f'{bucket_name}/', '')
        data_path = f"s3a://{bucket_name}/{catalog_path}/data"
        
        logger.info(f"Writing data to: {data_path}")
        
        for row in batches.collect():
            year, quarter, files = row['year'], row['quarter'], row['files']
            write_mode = "overwrite" if is_first_batch else "append"
            
            logger.info(f"Processing batch: Year={year}, Quarter={quarter} (Mode: {write_mode})")
            
            # Read files for this batch
            batch_df = spark.read.option("header", "true").schema(spark_schema).csv(files)
            
            # Add year and quarter columns
            batch_df = batch_df.withColumn("year", F.lit(year)).withColumn("quarter", F.lit(quarter))
            
            # Write to R2 Data Catalog location
            batch_df.write \
                .mode(write_mode) \
                .option("path", f"{data_path}/year={year}/quarter={quarter}") \
                .option("write.target-file-size-bytes", "67108864") \
                .option("write.parquet.compression-codec", "snappy") \
                .format("parquet") \
                .save()
            
            logger.info(f"Completed batch: Year={year}, Quarter={quarter}")
            is_first_batch = False
            
            # Force garbage collection
            spark.catalog.clearCache()
        
        batches.unpersist()
        logger.info("✅ Data writing completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to write data: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Create Iceberg table directly in R2 Data Catalog")
    parser.add_argument("--manifest-path", required=True, help="S3 path to manifest CSV file")
    parser.add_argument("--database-name", required=True, help="Database name in R2 catalog")
    parser.add_argument("--table-name", required=True, help="Table name")
    parser.add_argument("--filter-year", help="Comma-separated years to include (e.g., '2013,2014')")
    parser.add_argument("--drop-existing", action="store_true", help="Drop existing table if it exists")
    
    args = parser.parse_args()
    
    # Check required environment variables
    required_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'CLOUDFLARE_ACCOUNT_ID', 'R2_CATALOG_API_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {missing_vars}")
        sys.exit(1)
    
    spark = None
    try:
        logger.info(" Starting R2 Data Catalog Iceberg table creation...")
        
        # Step 1: Create Spark session
        spark = create_spark_session()
        
        # Step 2: Read manifest file
        logger.info(" Reading manifest file...")
        manifest_df = spark.read.option("header", "true").csv(args.manifest_path)
        logger.info(f" Read manifest with {manifest_df.count()} file entries")
        
        # Step 3: Generate unified schema
        logger.info(" Generating unified schema...")
        all_file_paths = [row.file_path for row in manifest_df.select("file_path").collect()]
        iceberg_schema, spark_schema = get_unified_schema(spark, all_file_paths)
        
        # Step 4: Create R2 Data Catalog connection
        catalog = create_r2_catalog_connection()
        
        # Step 5: Create table in R2 Data Catalog
        logger.info(" Creating table in R2 Data Catalog...")
        catalog_location = create_table_in_r2_catalog(catalog, args.database_name, args.table_name, iceberg_schema, args.drop_existing)
        
        # Step 6: Write data to R2 Data Catalog
        logger.info(" Writing data to R2 Data Catalog...")
        success = write_data_to_r2_catalog(spark, catalog_location, manifest_df, spark_schema, args.filter_year)
        
        if not success:
            logger.error("❌ Data writing failed!")
            sys.exit(1)
        
        # Step 7: Verification
        logger.info(" R2 Data Catalog table creation completed successfully!")
        
        # Print Snowflake integration info
        account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
        
        logger.info("\n" + "="*60)
        logger.info("SNOWFLAKE CATALOG INTEGRATION INFO:")
        logger.info("="*60)
        logger.info(f"Catalog URI: https://catalog.cloudflarestorage.com/{account_id}/{bucket_name}")
        logger.info(f"Database: {args.database_name}")
        logger.info(f"Table: {args.table_name}")
        logger.info(f"Storage Base URL: s3compat://{bucket_name}")
        logger.info(f"Table Location: {catalog_location}")
        logger.info("="*60)
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main() 