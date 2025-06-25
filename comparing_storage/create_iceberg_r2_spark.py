"""
High-Performance Spark Script for Single EC2 Instance - R2 Data Catalog Migration
=================================================================================

This script uses Spark on a single EC2 instance to migrate data from S3 CSV 
to Cloudflare R2 with Iceberg format via R2 Data Catalog.

Features:
- Uses pre-downloaded JAR files from r2/jars directory
- Auto-detects CPU cores and memory for optimal configuration
- Direct Python execution (no spark-submit needed)
- All Spark configs embedded in the code

Prerequisites:
1. Run download_iceberg_jars.sh in the r2 directory first
2. Set up environment variables in .env file
3. Install dependencies: pip install pyspark python-dotenv psutil

Usage:
    python create_iceberg_r2_spark_ec2.py --years 2013 2014
"""

import os
import sys
import gc
import argparse
import psutil
import multiprocessing
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, year as year_func
from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType, IntegerType
from dotenv import load_dotenv

load_dotenv()

def create_spark_session_single_instance(app_name="R2-Iceberg-Migration-EC2"):
    """
    Create Spark session optimized for single EC2 instance with pre-downloaded JARs
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
            # print(f"Found JAR: {jar}")
        else:
            print(f"Missing JAR: {jar_path}")
            print("Please run the download_iceberg_jars.sh script!")
            sys.exit(1)
    
    # Join all JAR paths
    jars_string = ",".join(jar_paths)
    print(f"Using {len(jar_paths)} JAR files")
    
    # Automatically detect and allocate memory based on available system memory
    try:
        available_memory_gb = psutil.virtual_memory().available // (1024**3)
        # Use 80% of available memory, minimum 4GB, maximum 28GB
        driver_memory_gb = max(4, min(28, int(available_memory_gb * 0.8)))
        print(f"Auto-detected memory: {available_memory_gb}GB available, using {driver_memory_gb}GB for Spark")
    except ImportError:
        driver_memory_gb = 8  # Default fallback
        print(f"Using default memory allocation: {driver_memory_gb}GB")
    
    return SparkSession.builder \
        .appName(app_name) \
        .master(f"local[{spark_cores}]") \
        .config("spark.jars", jars_string) \
        .config("spark.driver.memory", f"{driver_memory_gb}g") \
        .config("spark.driver.maxResultSize", f"{max(2, driver_memory_gb // 4)}g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "128MB") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", str(spark_cores * 4)) \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.unsafe", "true") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "8192") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.parquet.read.allocation.size", "67108864") \
        .config("spark.sql.files.maxPartitionBytes", "268435456") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.threads.max", "32") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4") \
        .config("spark.hadoop.fs.s3a.block.size", "67108864") \
        .config("spark.hadoop.fs.s3a.readahead.range", "524288") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.sql.shuffle.partitions", str(spark_cores * 8)) \
        .config("spark.default.parallelism", str(spark_cores * 4)) \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.sources.csv.enforceSchema", "false") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.r2_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.r2_catalog.type", "rest") \
        .config("spark.sql.catalog.r2_catalog.uri", os.getenv("R2_CATALOG_URI")) \
        .config("spark.sql.catalog.r2_catalog.warehouse", os.getenv("R2_WAREHOUSE")) \
        .config("spark.sql.catalog.r2_catalog.token", os.getenv("R2_CATALOG_API_TOKEN")) \
        .config("spark.sql.catalog.r2_catalog.oauth2-server-uri", f"{os.getenv('R2_CATALOG_URI')}/oauth/tokens") \
        .config("spark.sql.catalog.r2_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.r2_catalog.s3.endpoint", os.getenv("R2_ENDPOINT")) \
        .config("spark.sql.catalog.r2_catalog.s3.access-key-id", os.getenv("R2_ACCESS_KEY_ID")) \
        .config("spark.sql.catalog.r2_catalog.s3.secret-access-key", os.getenv("R2_SECRET_ACCESS_KEY")) \
        .config("spark.sql.iceberg.data-prefetch.enabled", "true") \
        .config("spark.sql.iceberg.vectorization.enabled", "true") \
        .getOrCreate()

def configure_s3_credentials(spark):
    """Configure S3 credentials for source data access"""
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "false")

def get_optimized_schema():
    """
    Define optimized schema for hard drive data with proper data types
    """
    return StructType([
        StructField("date", DateType(), True),
        StructField("serial_number", StringType(), True),
        StructField("model", StringType(), True),
        StructField("capacity_bytes", LongType(), True),
        StructField("failure", IntegerType(), True),
        # SMART attributes - using appropriate data types
        StructField("smart_1_normalized", IntegerType(), True),
        StructField("smart_1_raw", LongType(), True),
        StructField("smart_2_normalized", IntegerType(), True),
        StructField("smart_2_raw", LongType(), True),
        StructField("smart_3_normalized", IntegerType(), True),
        StructField("smart_3_raw", LongType(), True),
        StructField("smart_4_normalized", IntegerType(), True),
        StructField("smart_4_raw", LongType(), True),
        StructField("smart_5_normalized", IntegerType(), True),
        StructField("smart_5_raw", LongType(), True),
        StructField("smart_7_normalized", IntegerType(), True),
        StructField("smart_7_raw", LongType(), True),
        StructField("smart_8_normalized", IntegerType(), True),
        StructField("smart_8_raw", LongType(), True),
        StructField("smart_9_normalized", IntegerType(), True),
        StructField("smart_9_raw", LongType(), True),
        StructField("smart_10_normalized", IntegerType(), True),
        StructField("smart_10_raw", LongType(), True),
        StructField("smart_11_normalized", IntegerType(), True),
        StructField("smart_11_raw", LongType(), True),
        StructField("smart_12_normalized", IntegerType(), True),
        StructField("smart_12_raw", LongType(), True),
        StructField("smart_13_normalized", IntegerType(), True),
        StructField("smart_13_raw", LongType(), True),
        StructField("smart_15_normalized", IntegerType(), True),
        StructField("smart_15_raw", LongType(), True),
        StructField("smart_183_normalized", IntegerType(), True),
        StructField("smart_183_raw", LongType(), True),
        StructField("smart_184_normalized", IntegerType(), True),
        StructField("smart_184_raw", LongType(), True),
        StructField("smart_187_normalized", IntegerType(), True),
        StructField("smart_187_raw", LongType(), True),
        StructField("smart_188_normalized", IntegerType(), True),
        StructField("smart_188_raw", LongType(), True),
        StructField("smart_189_normalized", IntegerType(), True),
        StructField("smart_189_raw", LongType(), True),
        StructField("smart_190_normalized", IntegerType(), True),
        StructField("smart_190_raw", LongType(), True),
        StructField("smart_191_normalized", IntegerType(), True),
        StructField("smart_191_raw", LongType(), True),
        StructField("smart_192_normalized", IntegerType(), True),
        StructField("smart_192_raw", LongType(), True),
        StructField("smart_193_normalized", IntegerType(), True),
        StructField("smart_193_raw", LongType(), True),
        StructField("smart_194_normalized", IntegerType(), True),
        StructField("smart_194_raw", LongType(), True),
        StructField("smart_195_normalized", IntegerType(), True),
        StructField("smart_195_raw", LongType(), True),
        StructField("smart_196_normalized", IntegerType(), True),
        StructField("smart_196_raw", LongType(), True),
        StructField("smart_197_normalized", IntegerType(), True),
        StructField("smart_197_raw", LongType(), True),
        StructField("smart_198_normalized", IntegerType(), True),
        StructField("smart_198_raw", LongType(), True),
        StructField("smart_199_normalized", IntegerType(), True),
        StructField("smart_199_raw", LongType(), True),
        StructField("smart_200_normalized", IntegerType(), True),
        StructField("smart_200_raw", LongType(), True),
        StructField("smart_201_normalized", IntegerType(), True),
        StructField("smart_201_raw", LongType(), True),
        StructField("smart_223_normalized", IntegerType(), True),
        StructField("smart_223_raw", LongType(), True),
        StructField("smart_224_normalized", IntegerType(), True),
        StructField("smart_224_raw", LongType(), True),
        StructField("smart_225_normalized", IntegerType(), True),
        StructField("smart_225_raw", LongType(), True),
        StructField("smart_226_normalized", IntegerType(), True),
        StructField("smart_226_raw", LongType(), True),
        StructField("smart_240_normalized", IntegerType(), True),
        StructField("smart_240_raw", LongType(), True),
        StructField("smart_241_normalized", IntegerType(), True),
        StructField("smart_241_raw", LongType(), True),
        StructField("smart_242_normalized", IntegerType(), True),
        StructField("smart_242_raw", LongType(), True),
    ])

def get_dynamic_schema(spark, file_paths, sample_size=50):
    """
    Generate unified schema from sample of files (like S3 script approach)
    """
    print(f"Generating dynamic schema from {sample_size} sample files across all years (including latest 2025 files)...")
    
    # Sample files from different years to capture schema evolution
    # Ensure we get files from the latest years which have the most columns
    total_files = len(file_paths)
    if total_files <= sample_size:
        sample_files = file_paths
    else:
        # Sample evenly across the entire dataset to capture schema changes
        step = total_files // sample_size
        sample_files = [file_paths[i] for i in range(0, total_files, step)][:sample_size]
        
        # Ensure we include some files from 2025 (which likely have the most columns)
        files_2025 = [f for f in file_paths if '2025' in f]
        if files_2025:
            # Replace some of the sampled files with 2025 files to ensure latest schema
            num_2025_files = min(5, len(files_2025))
            sample_files[-num_2025_files:] = files_2025[:num_2025_files]
    
    # Convert s3:// to s3a:// for sample files
    sample_files_s3a = []
    for file_path in sample_files:
        if file_path.startswith("s3://"):
            sample_files_s3a.append(file_path.replace("s3://", "s3a://", 1))
        else:
            sample_files_s3a.append(file_path)
    
    try:
        unified_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(sample_files_s3a)
        
        print("✅ Dynamic schema generated successfully")
        print(f"   Schema inferred from {len(sample_files)} files")
        
        # Show which years were sampled for schema inference
        sample_years = set()
        for f in sample_files:
            for year in ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']:
                if year in f:
                    sample_years.add(year)
                    break
        print(f"   Years sampled: {sorted(sample_years)}")
        print(f"   Total columns in unified schema: {len(unified_df.schema.fields)}")
        
        return unified_df.schema
    except Exception as e:
        print(f"❌ Failed to generate dynamic schema: {e}")
        print("   Falling back to predefined schema")
        return get_optimized_schema()

def read_manifest_file(spark, manifest_path):
    """Read and parse the manifest file to get list of data files"""
    # Convert s3:// to s3a:// for better compatibility
    if manifest_path.startswith("s3://"):
        manifest_path_s3a = manifest_path.replace("s3://", "s3a://", 1)
    else:
        manifest_path_s3a = manifest_path
    
    try:
        manifest_df = spark.read.option("header", "true").csv(manifest_path_s3a)
        first_column = manifest_df.columns[0]
        file_paths = [row[first_column] for row in manifest_df.collect()]
        
        print(f"Found {len(file_paths)} files in manifest")
        return file_paths
    except Exception as e:
        print(f"❌ Failed to read manifest file: {e}")
        raise

def detect_available_years(file_paths):
    """Auto-detect all available years from file paths"""
    years = set()
    year_patterns = ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']
    
    for file_path in file_paths:
        for year in year_patterns:
            if f"/{year}/" in file_path or f"_{year}/" in file_path or f"data_{year}/" in file_path or f"{year}-" in file_path:
                years.add(int(year))
                break
    
    return sorted(list(years))

def process_year_batch(spark, file_paths, year, output_table, schema, batch_size=50, drop_existing=False, years_list=None, use_dynamic_schema=False):
    """
    Process data files for a specific year with memory-optimized streaming
    Single instance needs smaller batches to avoid OOM
    """
    print(f"\n=== Processing Year {year} ===")
    
    # Filter files for the specific year - check multiple patterns
    year_files = []
    for f in file_paths:
        # Check for patterns like: /2016/, _2016/, data_2016/, or 2016-
        if (f"/{year}/" in f or f"_{year}/" in f or f"data_{year}/" in f or f"{year}-" in f):
            year_files.append(f)
    
    print(f"Processing {len(year_files)} files for year {year}")
    
    if not year_files:
        print(f"No files found for year {year}")
        return
    
    # Create table if it doesn't exist or drop if requested
    if drop_existing:
        try:
            spark.sql(f"DROP TABLE IF EXISTS r2_catalog.default.{output_table}")
            print(f"Dropped existing table: {output_table}")
        except Exception as e:
            print(f"Note: Could not drop table (may not exist): {e}")
    
    # Process files in smaller batches for memory efficiency
    total_files = len(year_files)
    total_rows_processed = 0
    
    for i in range(0, total_files, batch_size):
        batch_files = year_files[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
        
        # Convert s3:// paths to s3a:// for better compatibility
        batch_files_s3a = []
        for file_path in batch_files:
            if file_path.startswith("s3://"):
                batch_files_s3a.append(file_path.replace("s3://", "s3a://", 1))
            else:
                batch_files_s3a.append(file_path)
        
        # Read batch with optimized settings and schema
        if use_dynamic_schema:
            # Dynamic schema inference - use the unified schema to ensure consistency
            df = spark.read \
                .schema(schema) \
                .option("header", "true") \
                .option("mergeSchema", "false") \
                .option("pathGlobFilter", "*.csv") \
                .csv(batch_files_s3a)
        else:
            # Predefined schema (original approach)
            df = spark.read \
                .schema(schema) \
                .option("header", "true") \
                .option("mergeSchema", "false") \
                .option("pathGlobFilter", "*.csv") \
                .csv(batch_files_s3a)
        
        # CRITICAL: Record original row count for data validation
        original_row_count = df.count()
        print(f"   Original CSV data: {original_row_count:,} rows")
        
        # Validate data quality - stop if NULL dates found
        null_dates_count = df.filter(col("date").isNull()).count()
        if null_dates_count > 0:
            print(f"❌ ERROR: Found {null_dates_count} NULL dates - stopping execution")
            return
        
        # Add partitioning columns for better query performance
        # Extract year and calculate quarter from date
        df_with_partitions = df \
            .withColumn("year", year_func(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("quarter", 
                when(month(col("date")).between(1, 3), "Q1")
                .when(month(col("date")).between(4, 6), "Q2") 
                .when(month(col("date")).between(7, 9), "Q3")
                .when(month(col("date")).between(10, 12), "Q4")
                .otherwise("Q1")) \
            .withColumn("failure_flag", when(col("failure") == 1, "failed").otherwise("healthy"))
        
        # Check for NULL years (shouldn't happen with proper date validation)
        null_year_count = df_with_partitions.filter(col("year").isNull()).count()
        if null_year_count > 0:
            print(f"❌ ERROR: Found {null_year_count} NULL years - data issue detected")
            return
        
        # Get row count for this batch
        batch_row_count = df_with_partitions.count()
        total_rows_processed += batch_row_count
        
        # Validate row count
        if batch_row_count != original_row_count:
            print(f"❌ ERROR: Row count mismatch - Original: {original_row_count:,}, Final: {batch_row_count:,}")
            return
        
        # Write to Iceberg table
        if i == 0 and year == min(sorted(years_list or [year])):  # First batch - create table
            print(f"Creating Iceberg table: {output_table}")
            df_with_partitions.writeTo(f"r2_catalog.default.{output_table}") \
                .partitionedBy("year", "quarter") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "268435456") \
                .tableProperty("write.parquet.compression-codec", "snappy") \
                .tableProperty("write.parquet.row-group-size-bytes", "67108864") \
                .tableProperty("write.merge-schema", "true") \
                .createOrReplace()
        else:  # Append to existing table
            df_with_partitions.writeTo(f"r2_catalog.default.{output_table}") \
                .option("mergeSchema", "true") \
                .append()
        
        print(f"✅ Batch {batch_num}: {batch_row_count:,} rows written")
        
        # Force garbage collection to free memory
        df_with_partitions.unpersist()
        del df, df_with_partitions
        gc.collect()
    
    print(f"✅ Year {year} completed: {total_rows_processed:,} total rows processed")

def optimize_table(spark, table_name):
    """Run table optimization for better query performance"""
    print(f"Optimizing table: {table_name}")
    
    try:
        spark.sql(f"CALL r2_catalog.system.rewrite_data_files(table => 'default.{table_name}')")
        spark.sql(f"CALL r2_catalog.system.rewrite_manifests('default.{table_name}')")
        print("✅ Table optimization completed")
    except Exception as e:
        print(f"⚠️ Table optimization failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Single EC2 Spark Migration to R2 Iceberg")
    parser.add_argument("--output-table", default="hard_drive_data", help="Output Iceberg table name")
    parser.add_argument("--years", nargs="+", type=int, default=None, 
                       help="Years to process (e.g., --years 2013 2014). If not specified, processes all available years.")
    parser.add_argument("--batch-size", type=int, default=50, 
                       help="Number of files to process per batch (smaller for less memory)")
    parser.add_argument("--drop-existing", action="store_true", 
                       help="Drop existing table before processing")
    parser.add_argument("--optimize-table", action="store_true", 
                       help="Run table optimization after processing")
    parser.add_argument("--dynamic-schema", action="store_true", 
                       help="Use dynamic schema inference instead of predefined schema")
    
    args = parser.parse_args()
    
    # Validate environment variables
    required_env_vars = ["R2_CATALOG_URI", "R2_WAREHOUSE", "R2_CATALOG_API_TOKEN", "R2_ENDPOINT", 
                        "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "S3_HDD_MANIFEST_PATH"]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Get manifest path from environment variable
    manifest_path = os.getenv("S3_HDD_MANIFEST_PATH")
    
    print("Starting R2 Iceberg Migration")
    print(f"Table: {args.output_table} | Batch size: {args.batch_size}")
    
    # Record start time
    start_time = time.time()
    
    # Create optimized Spark session
    spark = create_spark_session_single_instance()
    configure_s3_credentials(spark)
    
    # Suppress verbose Spark logs and warnings
    spark.sparkContext.setLogLevel("FATAL")
    spark.conf.set("spark.sql.adaptive.logLevel", "ERROR")
    
    # Suppress specific Spark loggers
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.sql.execution.adaptive").setLevel(log4j.Level.FATAL)
    log4j.LogManager.getLogger("org.apache.spark.sql.execution").setLevel(log4j.Level.FATAL)
    log4j.LogManager.getLogger("org.apache.hadoop.fs.s3a").setLevel(log4j.Level.FATAL)
    log4j.LogManager.getLogger("org.apache.spark.sql.sources.csv").setLevel(log4j.Level.FATAL)
    
    try:
        # Get file list first
        file_paths = read_manifest_file(spark, manifest_path)
        
        # Auto-detect years if not specified
        if args.years is None:
            detected_years = detect_available_years(file_paths)
            print(f"Auto-detected years: {detected_years}")
            years_to_process = detected_years
        else:
            years_to_process = args.years
        
        # Update the display message
        # schema_type = "Dynamic" if args.dynamic_schema else "Predefined"
        print(f"Processing years: {years_to_process}")
        
        # Get schema based on user preference
        if args.dynamic_schema:
            schema = get_dynamic_schema(spark, file_paths)
        else:
            schema = get_optimized_schema()
        
        # Process each year
        for year in sorted(years_to_process):
            process_year_batch(
                spark=spark,
                file_paths=file_paths,
                year=year,
                output_table=args.output_table,
                schema=schema,
                batch_size=args.batch_size,
                drop_existing=(args.drop_existing and year == min(years_to_process)),
                years_list=years_to_process,
                use_dynamic_schema=args.dynamic_schema
            )
        
        # Optimize table if requested
        if args.optimize_table:
            optimize_table(spark, args.output_table)
        
        # Calculate total execution time
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Show final statistics
        print("\n=== Migration Complete ===")
        result = spark.sql(f"SELECT COUNT(*) as total_rows FROM r2_catalog.default.{args.output_table}")
        total_rows = result.collect()[0]['total_rows']
        print(f"✅ Total rows: {total_rows:,}")

        print(f"Total time: {total_duration}s")
        
        # Show partition distribution
        partition_stats = spark.sql(f"""
            SELECT year, quarter, COUNT(*) as row_count 
            FROM r2_catalog.default.{args.output_table} 
            GROUP BY year, quarter 
            ORDER BY year, quarter
        """)
        print("\nPartition Distribution:")
        partition_stats.show()
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 