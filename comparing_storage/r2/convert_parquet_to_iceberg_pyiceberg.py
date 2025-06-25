"""
Convert Parquet files on R2 to Iceberg tables using PyIceberg and R2 Data Catalog.

This script uses PyIceberg to properly create Iceberg tables from existing Parquet files.
PyIceberg is the recommended approach for writing Iceberg tables.
"""

import os
import sys
import logging
import argparse
from dotenv import load_dotenv
import duckdb

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, 
    BooleanType, DateType, TimestampType, DoubleType
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert R2 Parquet files to Iceberg tables using PyIceberg')
    parser.add_argument('--test-years', type=str, help='Comma-separated years to test (e.g., 2013,2014)')
    parser.add_argument('--drop-existing', action='store_true', help='Drop existing Iceberg tables')
    parser.add_argument('--chunk-size', type=int, default=100000, help='Chunk size for large uploads')
    parser.add_argument('--verify', action='store_true', help='Verify the Iceberg table')
    return parser.parse_args()

def get_env_config():
    """Get configuration from environment variables."""
    config = {}
    
    # R2 Configuration
    config['R2_ACCESS_KEY_ID'] = os.getenv('R2_ACCESS_KEY_ID')
    config['R2_SECRET_ACCESS_KEY'] = os.getenv('R2_SECRET_ACCESS_KEY')
    config['R2_ENDPOINT'] = os.getenv('R2_ENDPOINT')
    config['R2_BUCKET_NAME'] = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
    config['R2_WAREHOUSE_PATH'] = os.getenv('R2_WAREHOUSE_PATH', 'warehouse')
    config['R2_TABLE_NAME'] = os.getenv('R2_TABLE_NAME', 'hard_drive_data')
    
    # R2 Data Catalog Configuration
    config['R2_CATALOG_URI'] = os.getenv('R2_CATALOG_URI')
    config['R2_CATALOG_API_TOKEN'] = os.getenv('R2_CATALOG_API_TOKEN')
    
    # Validate required config
    required_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_ENDPOINT', 'R2_CATALOG_URI', 'R2_CATALOG_API_TOKEN']
    missing_vars = [var for var in required_vars if not config.get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return config

def setup_duckdb_connection(config):
    """Set up DuckDB connection for reading Parquet files."""
    logger.info("Setting up DuckDB connection for reading Parquet files...")
    
    con = duckdb.connect(':memory:')
    
    # Install and load required extensions
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    
    # Configure R2 connection
    configure_r2_for_operation(con, config)
    
    logger.info("DuckDB connection configured successfully")
    return con

def configure_r2_for_operation(con, config):
    """Configure DuckDB's S3 settings for Cloudflare R2."""
    r2_endpoint = config.get('R2_ENDPOINT')
    if not r2_endpoint:
        raise ValueError("R2_ENDPOINT must be set in the environment for R2 operations")
        
    # DuckDB's S3 extension prepends https://, so we need just the host.
    endpoint_host = r2_endpoint.split('://')[-1]

    logger.info(f"Configuring for R2 operation: endpoint_host={endpoint_host}")
    
    con.execute(f"SET s3_access_key_id = '{config['R2_ACCESS_KEY_ID']}'")
    con.execute(f"SET s3_secret_access_key = '{config['R2_SECRET_ACCESS_KEY']}'")
    con.execute(f"SET s3_endpoint = '{endpoint_host}'")
    con.execute("SET s3_url_style = 'path'")
    con.execute("SET s3_use_ssl = true")

def read_parquet_data(con, config, test_years=None):
    """Read Parquet data from R2 using DuckDB."""
    logger.info("Reading Parquet data from R2...")
    
    # Use the glob pattern to read all parquet files
    glob_pattern = f"s3://{config['R2_BUCKET_NAME']}/{config['R2_WAREHOUSE_PATH']}/{config['R2_TABLE_NAME']}/**/*.parquet"
    
    # Read data into a DataFrame
    query = f"SELECT * FROM read_parquet('{glob_pattern}')"
    
    if test_years:
        years_list = "', '".join(test_years)
        query += f" WHERE year IN ('{years_list}')"
    
    logger.info(f"Executing query: {query[:100]}...")
    
    df = con.execute(query).df()
    logger.info(f"Read {len(df):,} rows with {len(df.columns)} columns")
    
    return df

def create_iceberg_schema(df):
    """Create Iceberg schema from DataFrame."""
    logger.info("Creating Iceberg schema...")
    
    # Map pandas dtypes to Iceberg types
    type_mapping = {
        'object': StringType(),
        'string': StringType(),
        'int64': LongType(),
        'int32': IntegerType(),
        'float64': DoubleType(),
        'bool': BooleanType(),
        'datetime64[ns]': TimestampType(),
        'date': DateType(),
    }
    
    fields = []
    for col_name, dtype in df.dtypes.items():
        iceberg_type = type_mapping.get(str(dtype), StringType())
        fields.append(NestedField.optional(len(fields) + 1, col_name, iceberg_type))
    
    schema = Schema(*fields)
    logger.info(f"Created schema with {len(fields)} fields")
    return schema

def setup_pyiceberg_catalog(config):
    """Set up PyIceberg catalog for R2 Data Catalog using official Cloudflare format."""
    logger.info("Setting up PyIceberg catalog...")
    
    # Extract warehouse from catalog URI
    # Catalog URI format: https://catalog.cloudflarestorage.com/[account-id]/[bucket-name]
    catalog_uri = config['R2_CATALOG_URI']
    account_id = catalog_uri.split('/')[-2]
    bucket_name = catalog_uri.split('/')[-1]
    
    # Warehouse format: [account-id]_[bucket-name]
    warehouse = f"{account_id}_{bucket_name}"
    
    # Connect to R2 Data Catalog using RestCatalog
    catalog = RestCatalog(
        name="r2_catalog",
        warehouse=warehouse,
        uri=catalog_uri,
        token=config['R2_CATALOG_API_TOKEN'],
    )
    
    logger.info("PyIceberg catalog configured successfully")
    return catalog

def clean_dataframe_for_iceberg(df):
    """Clean DataFrame to handle issues with PyArrow/Iceberg conversion."""
    logger.info("Cleaning DataFrame for Iceberg compatibility...")
    
    original_columns = len(df.columns)
    
    # Handle columns with all null values - DON'T DROP them, convert to string type
    null_columns = []
    for col in df.columns:
        if df[col].isnull().all():
            null_columns.append(col)
            # Convert to string type to preserve column for future data
            df[col] = df[col].astype('object').fillna('')
    
    if null_columns:
        logger.info(f"Converting {len(null_columns)} all-null columns to string type (preserving for future data): {null_columns[:5]}...")
    
    # Handle columns with mixed types by converting to string
    problematic_columns = []
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if it's truly mixed types
            non_null_sample = df[col].dropna()
            if len(non_null_sample) > 0:
                unique_types = set(type(x).__name__ for x in non_null_sample.iloc[:1000] if x is not None)
                if len(unique_types) > 1:
                    problematic_columns.append(col)
    
    if problematic_columns:
        logger.info(f"Converting {len(problematic_columns)} mixed-type columns to string: {problematic_columns[:5]}...")
        for col in problematic_columns:
            df[col] = df[col].astype(str)
    
    # Handle any remaining object columns by ensuring they're proper strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('').astype(str)
    
    logger.info(f"Cleaned DataFrame: {len(df):,} rows, {len(df.columns)} columns (preserved all {original_columns} original columns)")
    return df

def convert_to_iceberg(df, catalog, config, drop_existing=False, chunk_size=100000):
    """Convert DataFrame to Iceberg table using PyIceberg."""
    logger.info("Converting DataFrame to Iceberg table...")
    
    table_name = config['R2_TABLE_NAME']
    
    # Create default namespace if it doesn't exist
    try:
        catalog.create_namespace("default")
        logger.info("Created default namespace")
    except NamespaceAlreadyExistsError:
        logger.info("Default namespace already exists")
    
    # Clean DataFrame before conversion
    df_cleaned = clean_dataframe_for_iceberg(df)
    
    # Create table identifier
    table_identifier = ("default", table_name)
    
    # Handle drop existing table if requested
    if drop_existing:
        try:
            catalog.drop_table(table_identifier)
            logger.info(f"Dropped existing table: {table_name}")
        except Exception as e:
            logger.info(f"No existing table to drop: {e}")
    
    # Convert DataFrame to PyArrow table
    try:
        arrow_table = pa.Table.from_pandas(df_cleaned)
        logger.info(f"Converted DataFrame to PyArrow table with {len(arrow_table)} rows")
    except Exception as e:
        logger.error(f"Error converting to Arrow: {e}")
        logger.info("Attempting to fix data types...")
        
        # Force all object columns to string
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == 'object':
                df_cleaned[col] = df_cleaned[col].fillna('').astype(str)
        
        arrow_table = pa.Table.from_pandas(df_cleaned)
        logger.info(f"Successfully converted after type fixes: {len(arrow_table)} rows")
    
    # Check if we need to chunk the data for large uploads
    if len(arrow_table) > chunk_size:
        logger.info(f"Large dataset detected ({len(arrow_table):,} rows). Will process in chunks of {chunk_size:,}")
        
        # Create table with first chunk
        first_chunk = arrow_table.slice(0, chunk_size)
        
        try:
            table = catalog.create_table(
                table_identifier,
                schema=first_chunk.schema,
                properties={
                    'write.format.default': 'parquet',
                    'write.parquet.compression-codec': 'snappy'
                }
            )
            logger.info(f"Created new Iceberg table: {table_name}")
            
            # Write first chunk
            table.append(first_chunk)
            logger.info(f"Wrote first chunk: {len(first_chunk):,} rows")
            
            # Write remaining chunks
            #remaining_rows = len(arrow_table) - chunk_size
            chunks_written = 1
            
            for start in range(chunk_size, len(arrow_table), chunk_size):
                end = min(start + chunk_size, len(arrow_table))
                chunk = arrow_table.slice(start, end - start)
                
                table.append(chunk)
                chunks_written += 1
                logger.info(f"Wrote chunk {chunks_written}: {len(chunk):,} rows (total: {end:,}/{len(arrow_table):,})")
            
            logger.info(f"Successfully wrote all {chunks_written} chunks to Iceberg table!")
            
        except Exception as create_error:
            logger.error(f"Failed to create table: {create_error}")
            raise create_error
    
    else:
        # Small dataset, write all at once
        try:
            table = catalog.create_table(
                table_identifier,
                schema=arrow_table.schema,
                properties={
                    'write.format.default': 'parquet',
                    'write.parquet.compression-codec': 'snappy'
                }
            )
            logger.info(f"Created new Iceberg table: {table_name}")
            
            # Write data to the new table
            table.append(arrow_table)
            logger.info("Successfully wrote data to new Iceberg table!")
            
        except Exception as create_error:
            logger.error(f"Failed to create table: {create_error}")
            raise create_error

def verify_iceberg_table(catalog, config):
    """Verify the Iceberg table can be read."""
    logger.info("=== VERIFYING ICEBERG TABLE ===")
    
    try:
        table_name = config['R2_TABLE_NAME']
        table_identifier = ("default", table_name)
        table = catalog.load_table(table_identifier)
        
        # Get table info
        logger.info(f"Table: {table_name}")
        logger.info(f"Schema: {table.schema}")
        logger.info(f"Properties: {table.properties}")
        
        # Try to read some data
        df = table.scan().to_arrow().to_pandas()
        logger.info(f"Successfully read {len(df):,} rows from Iceberg table")
        
        # Show sample data
        logger.info(f"Sample data:\n{df.head()}")
        
    except Exception as e:
        logger.error(f"Could not verify Iceberg table: {e}")

def main():
    """Main function to convert Parquet to Iceberg using PyIceberg."""
    logger.info("=== CONVERTING PARQUET TO ICEBERG USING PYICEBERG ===")
    
    # Parse command-line arguments
    args = parse_arguments()
    config = get_env_config()
    
    try:
        # 1. Set up DuckDB to read Parquet files
        con = setup_duckdb_connection(config)
        
        # 2. Read Parquet data
        test_years = None
        if args.test_years:
            test_years = [year.strip() for year in args.test_years.split(',')]
        
        df = read_parquet_data(con, config, test_years)
        
        if len(df) == 0:
            logger.error("No data found to convert")
            return
        
        # 3. Set up PyIceberg catalog
        catalog = setup_pyiceberg_catalog(config)
        
        # 4. Convert to Iceberg
        convert_to_iceberg(df, catalog, config, args.drop_existing, args.chunk_size)
        
        if args.verify:
            verify_iceberg_table(catalog, config)
        
        logger.info("=== CONVERSION COMPLETED SUCCESSFULLY ===")
        logger.info("Your Iceberg table is now available in R2 Data Catalog")
        logger.info("You can use it with Snowflake external tables!")
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)
        
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    main() 