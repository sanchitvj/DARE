"""
Helper script to set up and validate Cloudflare R2 configuration for Iceberg migration.

This script helps you:
1. Validate R2 credentials and connectivity
2. Create necessary R2 buckets
3. Generate the correct migration command

Prerequisites:
- Install boto3: pip install boto3
- Have your Cloudflare R2 API credentials ready

Usage:
    # Set environment variables (secure method):
    export R2_ACCOUNT_ID=your-account-id
    export R2_ACCESS_KEY_ID=your-access-key
    export R2_SECRET_ACCESS_KEY=your-secret-key
    export R2_BUCKET_NAME=your-r2-bucket
    
    # Then run the script:
    python setup_r2_migration.py
    
    # Or validate connectivity only:
    python setup_r2_migration.py --validate-only
"""

import argparse
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
import sys
import os

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Setup and validate R2 configuration for Iceberg migration.")
    parser.add_argument("--r2-account-id", help="Cloudflare account ID (or set R2_ACCOUNT_ID env var)")
    parser.add_argument("--r2-access-key-id", help="R2 access key ID (or set R2_ACCESS_KEY_ID env var)")
    parser.add_argument("--r2-secret-access-key", help="R2 secret access key (or set R2_SECRET_ACCESS_KEY env var)")
    parser.add_argument("--r2-bucket-name", help="R2 bucket name for Iceberg data (or set R2_BUCKET_NAME env var)")
    parser.add_argument("--validate-only", action="store_true", help="Only validate credentials, don't create bucket")
    
    return parser.parse_args()

def create_r2_client(account_id, access_key_id, secret_access_key):
    """Create a boto3 S3 client configured for Cloudflare R2."""
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='auto'  # R2 uses 'auto' as the region
    )

def validate_r2_connection(r2_client):
    """Validate connection to R2 by listing buckets."""
    try:
        response = r2_client.list_buckets()
        print("✅ Successfully connected to Cloudflare R2")
        print(f"Found {len(response['Buckets'])} existing buckets")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']}")
        return True
    except EndpointConnectionError:
        print("❌ Failed to connect to R2. Check your internet connection and account ID.")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidAccessKeyId':
            print("❌ Invalid R2 access key ID")
        elif error_code == 'SignatureDoesNotMatch':
            print("❌ Invalid R2 secret access key")
        else:
            print(f"❌ R2 connection error: {error_code} - {e.response['Error']['Message']}")
        return False

def create_r2_bucket(r2_client, bucket_name):
    """Create R2 bucket if it doesn't exist."""
    try:
        # Check if bucket already exists
        r2_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Bucket doesn't exist, create it
            try:
                r2_client.create_bucket(Bucket=bucket_name)
                print(f"✅ Successfully created bucket '{bucket_name}'")
                return True
            except ClientError as create_error:
                print(f"❌ Failed to create bucket '{bucket_name}': {create_error}")
                return False
        else:
            print(f"❌ Error checking bucket '{bucket_name}': {e}")
            return False

def generate_migration_command(r2_config):
    """Generate the spark-submit command for migration."""
    
    print("\n" + "="*80)
    print("MIGRATION COMMAND")
    print("="*80)
    
    command = f"""spark-submit --deploy-mode cluster \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0 \\
    migrate_iceberg_s3_to_r2.py \\
    --source-table-identifier glue_catalog.your_db.your_table \\
    --dest-table-identifier r2_catalog.your_db.your_table \\
    --source-warehouse-path s3://your-current-iceberg-warehouse/ \\
    --dest-warehouse-path s3://{r2_config['bucket_name']}/ \\
    --r2-account-id $R2_ACCOUNT_ID \\
    --r2-access-key-id $R2_ACCESS_KEY_ID \\
    --r2-secret-access-key $R2_SECRET_ACCESS_KEY"""
    
    print(command)
    
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("1. Replace 'your_db.your_table' with your actual database and table names")
    print("2. Replace 's3://your-current-iceberg-warehouse/' with your actual S3 warehouse path")
    print("3. Ensure the environment variables are exported in your Spark cluster environment")
    print("4. Run the command on your Spark cluster (EMR, Databricks, etc.)")
    print("5. Monitor the job logs for any errors")
    
    print("\n" + "="*80)
    print("CURRENT ENVIRONMENT VARIABLES")
    print("="*80)
    print("✅ Your environment variables are already set:")
    print(f"R2_ACCOUNT_ID={r2_config['account_id']}")
    print(f"R2_ACCESS_KEY_ID={r2_config['access_key_id'][:8]}...")
    print("R2_SECRET_ACCESS_KEY=***hidden***")
    print(f"R2_BUCKET_NAME={r2_config['bucket_name']}")

def get_r2_config(args):
    """Get R2 configuration from command line args or environment variables."""
    # Priority: command line args > environment variables
    r2_account_id = args.r2_account_id or os.getenv('R2_ACCOUNT_ID')
    r2_access_key_id = args.r2_access_key_id or os.getenv('R2_ACCESS_KEY_ID')
    r2_secret_access_key = args.r2_secret_access_key or os.getenv('R2_SECRET_ACCESS_KEY')
    r2_bucket_name = args.r2_bucket_name or os.getenv('R2_BUCKET_NAME')
    
    # Validate all required values are present
    missing_vars = []
    if not r2_account_id:
        missing_vars.append("R2_ACCOUNT_ID")
    if not r2_access_key_id:
        missing_vars.append("R2_ACCESS_KEY_ID")
    if not r2_secret_access_key:
        missing_vars.append("R2_SECRET_ACCESS_KEY")
    if not r2_bucket_name:
        missing_vars.append("R2_BUCKET_NAME")
    
    if missing_vars:
        print("❌ Missing required R2 configuration:")
        for var in missing_vars:
            print(f"   - {var} (set as environment variable or use --{var.lower().replace('_', '-')} flag)")
        print("\nExample environment setup:")
        print("export R2_ACCOUNT_ID=your-account-id")
        print("export R2_ACCESS_KEY_ID=your-access-key")
        print("export R2_SECRET_ACCESS_KEY=your-secret-key")
        print("export R2_BUCKET_NAME=your-bucket-name")
        return None
    
    return {
        'account_id': r2_account_id,
        'access_key_id': r2_access_key_id,
        'secret_access_key': r2_secret_access_key,
        'bucket_name': r2_bucket_name
    }

def main():
    """Main function to setup R2 migration."""
    args = parse_arguments()
    
    print("Setting up Cloudflare R2 for Iceberg migration...")
    
    # Get R2 configuration from args or environment
    r2_config = get_r2_config(args)
    if not r2_config:
        sys.exit(1)
    
    print(f"Account ID: {r2_config['account_id']}")
    print(f"Bucket: {r2_config['bucket_name']}")
    print("✅ Using environment variables for credentials (secure!)")
    print()
    
    # Create R2 client
    try:
        r2_client = create_r2_client(
            r2_config['account_id'], 
            r2_config['access_key_id'], 
            r2_config['secret_access_key']
        )
    except Exception as e:
        print(f"❌ Failed to create R2 client: {e}")
        sys.exit(1)
    
    # Validate connection
    if not validate_r2_connection(r2_client):
        print("\nPlease check your R2 credentials and try again.")
        sys.exit(1)
    
    # Create bucket if needed
    if not args.validate_only:
        if not create_r2_bucket(r2_client, r2_config['bucket_name']):
            print("\nBucket creation failed. You may need to create it manually in the Cloudflare dashboard.")
            sys.exit(1)
    
    # Generate migration command
    generate_migration_command(r2_config)
    
    print("\n✅ R2 setup completed successfully!")

if __name__ == "__main__":
    main() 