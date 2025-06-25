"""
Clean R2 bucket completely before fresh migration
"""

import os
import boto3
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_r2_bucket():
    """Clean all objects from R2 bucket"""
    
    # Get credentials
    access_key = os.getenv('R2_ACCESS_KEY_ID')
    secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
    account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
    bucket_name = os.getenv('R2_BUCKET_NAME')
    
    if not all([access_key, secret_key, account_id]):
        logger.error("❌ Missing required environment variables:")
        logger.error("   - R2_ACCESS_KEY_ID")
        logger.error("   - R2_SECRET_ACCESS_KEY") 
        logger.error("   - CLOUDFLARE_ACCOUNT_ID")
        return False
    
    try:
        # Create S3 client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto'
        )
        
        logger.info(f"Starting to clean R2 bucket: {bucket_name}")
        
        # List all objects
        logger.info("Listing all objects in bucket...")
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        total_objects = 0
        deleted_objects = 0
        
        for page in pages:
            if 'Contents' in page:
                objects = page['Contents']
                total_objects += len(objects)
                
                logger.info(f"Found {len(objects)} objects in this batch...")
                
                # Delete objects in batches of 1000 (S3 limit)
                for i in range(0, len(objects), 1000):
                    batch = objects[i:i+1000]
                    
                    # Prepare delete request
                    delete_keys = [{'Key': obj['Key']} for obj in batch]
                    
                    logger.info(f"Deleting batch of {len(delete_keys)} objects...")
                    
                    response = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': delete_keys}
                    )
                    
                    if 'Deleted' in response:
                        deleted_objects += len(response['Deleted'])
                        logger.info(f"✅ Deleted {len(response['Deleted'])} objects")
                    
                    if 'Errors' in response:
                        for error in response['Errors']:
                            logger.error(f"❌ Error deleting {error['Key']}: {error['Message']}")
        
        logger.info("Bucket cleaning completed!")
        logger.info(f"   Total objects found: {total_objects}")
        logger.info(f"   Objects deleted: {deleted_objects}")
        
        # Verify bucket is empty
        logger.info("Verifying bucket is empty...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        
        if 'Contents' not in response:
            logger.info("✅ Bucket is now completely empty!")
            return True
        else:
            logger.warning(f"⚠️ Bucket still contains {response['KeyCount']} objects")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to clean bucket: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting R2 bucket cleanup...")
    success = clean_r2_bucket()
    
    if success:
        logger.info("✅ R2 bucket is ready for fresh migration!")
    else:
        logger.error("❌ Bucket cleanup failed!")
        exit(1) 