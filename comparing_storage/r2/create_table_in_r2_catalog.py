"""
Create table in R2 Data Catalog and migrate existing data

This script:
1. Creates a table properly in R2 Data Catalog (so it gets the right location)
2. Copies your existing data to the R2 Data Catalog managed location
3. Registers the table for Snowflake integration
"""

import os
import logging
import argparse
import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_table_schema_from_existing_metadata(bucket_name: str, metadata_path: str, account_id: str):
    """Read schema from existing metadata file"""
    
    try:
        # Create S3 client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        # Read the metadata file
        logger.info(f"Reading metadata from: {metadata_path}")
        metadata_obj = s3_client.get_object(Bucket=bucket_name, Key=metadata_path)
        metadata_content = metadata_obj['Body'].read()
        
        import json
        metadata = json.loads(metadata_content.decode('utf-8'))
        
        logger.info(f" Metadata keys: {list(metadata.keys())}")
        
        # Iceberg metadata structure
        schema_info = metadata.get('schema', {})
        fields = schema_info.get('fields', [])
        
        # Debug the schema structure
        logger.info(f" Schema info keys: {list(schema_info.keys()) if schema_info else 'None'}")
        logger.info(f" Found schema with {len(fields)} fields")
        
        # If no fields, try alternative structure
        if not fields:
            # Sometimes schema is nested differently
            current_schema = metadata.get('current-schema-id')
            schemas = metadata.get('schemas', [])
            
            logger.info(f" Current schema ID: {current_schema}")
            logger.info(f" Available schemas: {len(schemas)}")
            
            if schemas:
                # Find the current schema
                for schema in schemas:
                    if schema.get('schema-id') == current_schema:
                        fields = schema.get('fields', [])
                        schema_info = schema
                        logger.info(f"✅ Found current schema with {len(fields)} fields")
                        break
                
                # If still no fields, use the first schema
                if not fields and schemas:
                    schema_info = schemas[0]
                    fields = schema_info.get('fields', [])
                    logger.info(f" Using first schema with {len(fields)} fields")
        
        # Show first few fields for debugging
        if fields:
            logger.info(" First few fields:")
            for i, field in enumerate(fields[:3]):
                logger.info(f"   {i+1}. {field.get('name', 'unknown')}: {field.get('type', 'unknown')}")
        
        return schema_info, fields
        
    except Exception as e:
        logger.error(f"Failed to read existing metadata: {e}")
        return None, None

def create_table_in_r2_catalog(table_name: str, database_name: str, schema_fields: list):
    """Create table in R2 Data Catalog with proper schema"""
    
    account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    api_token = os.getenv('R2_CATALOG_API_TOKEN')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
    
    try:
        from pyiceberg.catalog.rest import RestCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            NestedField, StringType, IntegerType, LongType, 
            BooleanType, DoubleType, FloatType, DateType, TimestampType
        )
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import IdentityTransform
        
        # R2 Data Catalog configuration
        catalog_uri = f"https://catalog.cloudflarestorage.com/{account_id}/{bucket_name}"
        warehouse = f"{account_id}_{bucket_name}"
        
        logger.info(" Connecting to R2 Data Catalog...")
        
        # Initialize catalog with working authentication
        catalog = RestCatalog(
            name="r2_catalog",
            **{
                "uri": catalog_uri,
                "token": api_token,  # This is the working method
                "warehouse": warehouse,
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
        
        # Convert schema fields to PyIceberg types
        logger.info("🔧 Converting schema...")
        
        iceberg_fields = []
        for i, field in enumerate(schema_fields):
            field_name = field.get('name', f'field_{i}')
            field_type_info = field.get('type', 'string')
            
            # Convert type
            if isinstance(field_type_info, dict):
                type_name = field_type_info.get('type', 'string')
            else:
                type_name = str(field_type_info).lower()
            
            # Map to PyIceberg types
            if 'string' in type_name:
                iceberg_type = StringType()
            elif 'int' in type_name or type_name == 'integer':
                iceberg_type = IntegerType()
            elif 'long' in type_name or 'bigint' in type_name:
                iceberg_type = LongType()
            elif 'bool' in type_name:
                iceberg_type = BooleanType()
            elif 'double' in type_name:
                iceberg_type = DoubleType()
            elif 'float' in type_name:
                iceberg_type = FloatType()
            elif 'date' in type_name:
                iceberg_type = DateType()
            elif 'timestamp' in type_name:
                iceberg_type = TimestampType()
            else:
                iceberg_type = StringType()  # Default fallback
            
            iceberg_fields.append(
                NestedField(
                    field_id=field.get('id', i + 1),
                    name=field_name,
                    field_type=iceberg_type,
                    required=field.get('required', True)
                )
            )
        
        # Create schema
        schema = Schema(*iceberg_fields)
        logger.info(f"📊 Created schema with {len(iceberg_fields)} fields")
        
        # Create partition spec (year, quarter)
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=next((f.field_id for f in iceberg_fields if f.name == 'year'), 1),
                field_id=1000,
                transform=IdentityTransform(),
                name='year'
            ),
            PartitionField(
                source_id=next((f.field_id for f in iceberg_fields if f.name == 'quarter'), 2),
                field_id=1001,
                transform=IdentityTransform(),
                name='quarter'
            )
        )
        
        # Create table
        table_identifier = (database_name, table_name)
        
        logger.info(f"🗂️ Creating table: {'.'.join(table_identifier)}")
        
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
        return None

def copy_data_to_r2_catalog_location(source_location: str, target_location: str, account_id: str):
    """Copy data from existing location to R2 Data Catalog managed location"""
    
    try:
        # Create S3 client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
            region_name='auto'
        )
        
        bucket_name = source_location.replace('s3://', '').split('/')[0]
        source_prefix = source_location.replace(f's3://{bucket_name}/', '') + '/data/'
        target_prefix = target_location.replace(f's3://{bucket_name}/', '') + '/data/'
        
        logger.info("Copying data...")
        logger.info(f"   From: {source_prefix}")
        logger.info(f"   To: {target_prefix}")
        
        # List all data files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)
        
        copied_files = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key']
                    
                    # Skip if not a data file
                    if not source_key.endswith('.parquet'):
                        continue
                    
                    # Create target key
                    relative_path = source_key.replace(source_prefix, '')
                    target_key = target_prefix + relative_path
                    
                    # Copy object
                    copy_source = {'Bucket': bucket_name, 'Key': source_key}
                    s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket_name,
                        Key=target_key
                    )
                    
                    copied_files += 1
                    if copied_files % 10 == 0:
                        logger.info(f"   Copied {copied_files} files...")
        
        logger.info(f"✅ Copied {copied_files} data files to R2 Data Catalog location")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to copy data: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Create table in R2 Data Catalog and migrate data")
    parser.add_argument("--table-name", required=True, help="Table name (e.g., 'hard_drive_data')")
    parser.add_argument("--source-location", required=True, help="Current table location")
    parser.add_argument("--database", default="hdd_r2", help="Database name in R2 catalog")
    
    args = parser.parse_args()
    
    account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'hdd-iceberg-r2')
    
    logger.info(" Creating table in R2 Data Catalog...")
    logger.info(f"Table: {args.table_name}")
    logger.info(f"Source: {args.source_location}")
    logger.info(f"Database: {args.database}")
    
    # Step 1: Read existing schema
    logger.info(" Step 1: Reading existing table schema...")
    metadata_path = args.source_location.replace(f's3://{bucket_name}/', '') + '/metadata/v8.metadata.json'
    schema_info, fields = get_table_schema_from_existing_metadata(bucket_name, metadata_path, account_id)
    
    if not fields:
        logger.error("❌ Could not read existing schema")
        exit(1)
    
    # Step 2: Create table in R2 Data Catalog
    logger.info("Step 2: Creating table in R2 Data Catalog...")
    catalog_location = create_table_in_r2_catalog(args.table_name, args.database, fields)
    
    if not catalog_location:
        logger.error("❌ Could not create table in R2 Data Catalog")
        exit(1)
    
    # Step 3: Copy data to catalog location
    logger.info("Step 3: Copying data to R2 Data Catalog location...")
    success = copy_data_to_r2_catalog_location(args.source_location, catalog_location, account_id)
    
    if success:
        logger.info("🎉 Table creation and data migration completed!")
        logger.info("✅ Your table is now properly registered in R2 Data Catalog!")
        
        # Print Snowflake connection info
        logger.info("\n" + "="*60)
        logger.info("SNOWFLAKE CATALOG INTEGRATION INFO:")
        logger.info("="*60)
        logger.info(f"Catalog URI: https://catalog.cloudflarestorage.com/{account_id}/{bucket_name}")
        logger.info(f"Database: {args.database}")
        logger.info(f"Table: {args.table_name}")
        logger.info(f"Storage Base URL: s3compat://{bucket_name}")
        logger.info(f"Table Location: {catalog_location}")
        logger.info("="*60)
    else:
        logger.error("❌ Data migration failed!")
        exit(1)

if __name__ == "__main__":
    main() 