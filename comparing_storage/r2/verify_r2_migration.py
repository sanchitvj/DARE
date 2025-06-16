#!/usr/bin/env python3
"""
Verify R2 Iceberg Migration

This script verifies the migrated Iceberg data in R2 by running various queries
and comparing results with the source data in S3.
"""

import argparse
import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_spark_session(r2_bucket: str) -> SparkSession:
    """Create Spark session with both Glue and R2 catalogs."""
    print("Creating Spark session...")
    
    # JAR files for Iceberg
    iceberg_jars = [
        "jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
        "jars/bundle-2.20.18.jar", 
        "jars/url-connection-client-2.20.18.jar",
        "jars/hadoop-aws-3.3.4.jar",
        "jars/aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    spark = SparkSession.builder \
        .appName("VerifyIcebergR2Migration") \
        .config("spark.jars", ",".join(iceberg_jars)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
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
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created successfully")
    return spark

def verify_migration(spark: SparkSession, source_table: str, dest_table: str, filter_years: str = None):
    """Verify the migration by comparing source and destination data."""
    print("\n" + "="*60)
    print("🔍 VERIFICATION RESULTS")
    print("="*60)
    
    # Build year filter if provided
    year_filter = ""
    if filter_years:
        years = [year.strip() for year in filter_years.split(",")]
        year_conditions = " OR ".join([f"year = {year}" for year in years])
        year_filter = f" WHERE {year_conditions}"
    
    try:
        # 1. Record counts
        print("\n1️⃣ RECORD COUNTS:")
        print("-" * 30)
        
        source_count = spark.sql(f"SELECT COUNT(*) as count FROM glue.{source_table}{year_filter}").collect()[0]['count']
        dest_count = spark.sql(f"SELECT COUNT(*) as count FROM r2.{dest_table}{year_filter}").collect()[0]['count']
        
        print(f"Source (S3):  {source_count:,} records")
        print(f"Dest (R2):    {dest_count:,} records")
        print(f"Match:        {'✅ YES' if source_count == dest_count else '❌ NO'}")
        
        # 2. Schema comparison
        print("\n2️⃣ SCHEMA COMPARISON:")
        print("-" * 30)
        
        source_schema = spark.sql(f"SELECT * FROM glue.{source_table} LIMIT 0").columns
        dest_schema = spark.sql(f"SELECT * FROM r2.{dest_table} LIMIT 0").columns
        
        print(f"Source columns: {len(source_schema)}")
        print(f"Dest columns:   {len(dest_schema)}")
        print(f"Schema match:   {'✅ YES' if source_schema == dest_schema else '❌ NO'}")
        
        # 3. Data by year/quarter
        print("\n3️⃣ DATA BY YEAR/QUARTER:")
        print("-" * 30)
        
        print("SOURCE (S3):")
        source_breakdown = spark.sql(f"""
            SELECT year, quarter, COUNT(*) as records 
            FROM glue.{source_table}{year_filter}
            GROUP BY year, quarter 
            ORDER BY year, quarter
        """)
        source_breakdown.show()
        
        print("DESTINATION (R2):")
        dest_breakdown = spark.sql(f"""
            SELECT year, quarter, COUNT(*) as records 
            FROM r2.{dest_table}{year_filter}
            GROUP BY year, quarter 
            ORDER BY year, quarter
        """)
        dest_breakdown.show()
        
        # 4. Sample data verification
        print("\n4️⃣ SAMPLE DATA VERIFICATION:")
        print("-" * 30)
        
        # Get first few records and compare key fields
        source_sample = spark.sql(f"""
            SELECT date, serial_number, model, failure, year, quarter 
            FROM glue.{source_table}{year_filter}
            ORDER BY date, serial_number 
            LIMIT 5
        """)
        
        dest_sample = spark.sql(f"""
            SELECT date, serial_number, model, failure, year, quarter 
            FROM r2.{dest_table}{year_filter}
            ORDER BY date, serial_number 
            LIMIT 5
        """)
        
        print("SOURCE sample:")
        source_sample.show()
        
        print("DESTINATION sample:")
        dest_sample.show()
        
        # 5. File locations and metadata
        print("\n5️⃣ TABLE METADATA:")
        print("-" * 30)
        
        print("R2 table location:")
        r2_location = spark.sql(f"DESCRIBE TABLE EXTENDED r2.{dest_table}").filter("col_name = 'Location'").collect()
        if r2_location:
            print(f"  {r2_location[0]['data_type']}")
        
        print("R2 table format:")
        r2_format = spark.sql(f"DESCRIBE TABLE EXTENDED r2.{dest_table}").filter("col_name = 'Provider'").collect()
        if r2_format:
            print(f"  {r2_format[0]['data_type']}")
            
        # 6. Summary
        print("\n6️⃣ MIGRATION SUMMARY:")
        print("-" * 30)
        
        all_good = (source_count == dest_count and source_schema == dest_schema)
        
        if all_good:
            print("🎉 MIGRATION SUCCESSFUL!")
            print("✅ Record counts match")
            print("✅ Schemas match")
            print("✅ Data appears intact")
        else:
            print("⚠️  MIGRATION ISSUES DETECTED!")
            if source_count != dest_count:
                print("❌ Record count mismatch")
            if source_schema != dest_schema:
                print("❌ Schema mismatch")
        
        return all_good
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Verify Iceberg migration from S3 to R2")
    parser.add_argument("--source-table", required=True, help="Source table name (e.g., 'db.table')")
    parser.add_argument("--dest-table", required=True, help="Destination table name (e.g., 'db.table')")
    parser.add_argument("--r2-bucket", required=True, help="R2 bucket name")
    parser.add_argument("--filter-years", help="Comma-separated years to verify (e.g., '2013,2014')")
    
    args = parser.parse_args()
    
    # Check required environment variables
    required_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'CLOUDFLARE_ACCOUNT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        sys.exit(1)
    
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session(args.r2_bucket)
        
        # Verify migration
        success = verify_migration(spark, args.source_table, args.dest_table, args.filter_years)
        
        if success:
            print("\n🎉 VERIFICATION PASSED!")
            sys.exit(0)
        else:
            print("\n❌ VERIFICATION FAILED!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main() 