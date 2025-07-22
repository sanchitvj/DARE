"""
Cloudflare R2 Model Context Protocol (MCP) Server

This server enables AI agents to interact with Cloudflare R2 storage through
the Model Context Protocol (MCP). It provides S3-compatible operations for
file management, metadata operations, and data catalog functionality.

Features:
- List buckets and objects
- Upload/download files
- Delete objects and buckets
- Get object metadata and properties
- Search objects by prefix/pattern
- Generate presigned URLs
- Catalog operations for data discovery

Usage:
    python r2_server.py [log_file_path]
"""

import os
import asyncio
import logging
import json
from typing import Optional, Any, Dict, List, Union
import sys
from datetime import datetime, timedelta
import base64
import mimetypes

import boto3
from dotenv import load_dotenv
import mcp.server.stdio
from mcp.server import Server
from mcp.types import Tool, TextContent


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('r2_server')

# Add file logging
if len(sys.argv) > 1:
    log_path = sys.argv[1]
else:
    log_path = 'r2_mcp_server.log'
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

load_dotenv()


class R2Connection:
    """Manages Cloudflare R2 connections and operations using S3-compatible API."""
    
    def __init__(self) -> None:
        """Initialize R2 connection configuration from environment variables."""
        self.endpoint = os.getenv('R2_ENDPOINT')
        self.access_key = os.getenv('R2_ACCESS_KEY_ID')
        self.secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
        self.region = os.getenv('R2_REGION', 'enam')
        self.default_bucket = os.getenv('R2_BUCKET_NAME')
        
        if not all([self.endpoint, self.access_key, self.secret_key]):
            raise ValueError("Missing required R2 credentials. Please set R2_ENDPOINT, R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY")
        
        self.client: Optional[boto3.client] = None
        logger.info("R2Connection initialized")
    
    def get_client(self):
        """Get or create R2 S3-compatible client."""
        if self.client is None:
            try:
                self.client = boto3.client(
                    's3',
                    endpoint_url=self.endpoint,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=self.region
                )
                logger.info("R2 client created successfully")
            except Exception as e:
                logger.error(f"Failed to create R2 client: {e}")
                raise
        return self.client
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all R2 buckets."""
        try:
            client = self.get_client()
            response = client.list_buckets()
            buckets = []
            for bucket in response.get('Buckets', []):
                buckets.append({
                    'name': bucket['Name'],
                    'creation_date': bucket['CreationDate'].isoformat(),
                })
            logger.info(f"Listed {len(buckets)} buckets")
            return buckets
        except Exception as e:
            logger.error(f"Error listing buckets: {e}")
            raise
    
    def list_objects(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> Dict[str, Any]:
        """List objects in an R2 bucket."""
        try:
            client = self.get_client()
            kwargs = {
                'Bucket': bucket,
                'MaxKeys': max_keys
            }
            if prefix:
                kwargs['Prefix'] = prefix
                
            response = client.list_objects_v2(**kwargs)
            
            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'].strip('"'),
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
            
            result = {
                'objects': objects,
                'count': len(objects),
                'truncated': response.get('IsTruncated', False),
                'next_token': response.get('NextContinuationToken')
            }
            
            logger.info(f"Listed {len(objects)} objects from bucket {bucket}")
            return result
        except Exception as e:
            logger.error(f"Error listing objects in bucket {bucket}: {e}")
            raise
    
    def get_object_metadata(self, bucket: str, key: str) -> Dict[str, Any]:
        """Get metadata for a specific object."""
        try:
            client = self.get_client()
            response = client.head_object(Bucket=bucket, Key=key)
            
            metadata = {
                'key': key,
                'bucket': bucket,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'etag': response['ETag'].strip('"'),
                'content_type': response.get('ContentType', ''),
                'metadata': response.get('Metadata', {}),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'server_side_encryption': response.get('ServerSideEncryption'),
                'version_id': response.get('VersionId')
            }
            
            logger.info(f"Retrieved metadata for {bucket}/{key}")
            return metadata
        except Exception as e:
            logger.error(f"Error getting metadata for {bucket}/{key}: {e}")
            raise
    
    def upload_file(self, bucket: str, key: str, content: Union[str, bytes], 
                   content_type: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Upload a file to R2."""
        try:
            client = self.get_client()
            
            # Convert string content to bytes if needed
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            # Auto-detect content type if not provided
            if not content_type:
                content_type, _ = mimetypes.guess_type(key)
                if not content_type:
                    content_type = 'application/octet-stream'
            
            kwargs = {
                'Bucket': bucket,
                'Key': key,
                'Body': content,
                'ContentType': content_type
            }
            
            if metadata:
                kwargs['Metadata'] = metadata
            
            response = client.put_object(**kwargs)
            
            result = {
                'bucket': bucket,
                'key': key,
                'size': len(content),
                'etag': response['ETag'].strip('"'),
                'version_id': response.get('VersionId'),
                'content_type': content_type,
                'uploaded_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Uploaded {key} to bucket {bucket} ({len(content)} bytes)")
            return result
        except Exception as e:
            logger.error(f"Error uploading {key} to bucket {bucket}: {e}")
            raise
    
    def download_file(self, bucket: str, key: str, as_text: bool = True) -> Dict[str, Any]:
        """Download a file from R2."""
        try:
            client = self.get_client()
            response = client.get_object(Bucket=bucket, Key=key)
            
            content = response['Body'].read()
            
            result = {
                'bucket': bucket,
                'key': key,
                'size': len(content),
                'content_type': response.get('ContentType', ''),
                'last_modified': response['LastModified'].isoformat(),
                'etag': response['ETag'].strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
            if as_text:
                try:
                    result['content'] = content.decode('utf-8')
                except UnicodeDecodeError:
                    result['content'] = base64.b64encode(content).decode('ascii')
                    result['content_encoding'] = 'base64'
            else:
                result['content'] = base64.b64encode(content).decode('ascii')
                result['content_encoding'] = 'base64'
            
            logger.info(f"Downloaded {key} from bucket {bucket} ({len(content)} bytes)")
            return result
        except Exception as e:
            logger.error(f"Error downloading {key} from bucket {bucket}: {e}")
            raise
    
    def delete_object(self, bucket: str, key: str) -> Dict[str, Any]:
        """Delete an object from R2."""
        try:
            client = self.get_client()
            response = client.delete_object(Bucket=bucket, Key=key)
            
            result = {
                'bucket': bucket,
                'key': key,
                'deleted': True,
                'version_id': response.get('VersionId'),
                'deleted_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Deleted {key} from bucket {bucket}")
            return result
        except Exception as e:
            logger.error(f"Error deleting {key} from bucket {bucket}: {e}")
            raise
    
    def generate_presigned_url(self, bucket: str, key: str, operation: str = 'get_object', 
                              expires_in: int = 3600) -> Dict[str, Any]:
        """Generate a presigned URL for R2 object operations."""
        try:
            client = self.get_client()
            
            url = client.generate_presigned_url(
                operation,
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expires_in
            )
            
            result = {
                'bucket': bucket,
                'key': key,
                'operation': operation,
                'url': url,
                'expires_in': expires_in,
                'expires_at': (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat()
            }
            
            logger.info(f"Generated presigned URL for {operation} on {bucket}/{key}")
            return result
        except Exception as e:
            logger.error(f"Error generating presigned URL for {bucket}/{key}: {e}")
            raise
    
    def search_objects(self, bucket: str, pattern: str, max_keys: int = 100) -> Dict[str, Any]:
        """Search for objects matching a pattern."""
        try:
            # For simplicity, we'll use prefix matching
            # In a more advanced implementation, you could use regex or other patterns
            objects = self.list_objects(bucket, prefix=pattern, max_keys=max_keys)
            
            # Filter objects that contain the pattern in their key
            filtered_objects = []
            for obj in objects['objects']:
                if pattern.lower() in obj['key'].lower():
                    filtered_objects.append(obj)
            
            result = {
                'bucket': bucket,
                'pattern': pattern,
                'objects': filtered_objects,
                'count': len(filtered_objects)
            }
            
            logger.info(f"Found {len(filtered_objects)} objects matching pattern '{pattern}' in bucket {bucket}")
            return result
        except Exception as e:
            logger.error(f"Error searching objects in bucket {bucket} with pattern '{pattern}': {e}")
            raise


# Global R2 connection instance
r2_conn = R2Connection()

# Create MCP server instance
server = Server("r2-mcp-server")

# Define available tools
@server.list_tools()
async def list_tools() -> List[Tool]:
    """List all available R2 MCP tools."""
    return [
        Tool(
            name="list_buckets",
            description="List all R2 buckets in the account",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="list_objects",
            description="List objects in an R2 bucket",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "prefix": {
                        "type": "string",
                        "description": "Optional prefix to filter objects",
                        "default": ""
                    },
                    "max_keys": {
                        "type": "integer",
                        "description": "Maximum number of objects to return",
                        "default": 1000,
                        "minimum": 1,
                        "maximum": 1000
                    }
                },
                "required": ["bucket"]
            }
        ),
        Tool(
            name="get_object_metadata",
            description="Get metadata for a specific object in R2",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "key": {
                        "type": "string",
                        "description": "Object key/path"
                    }
                },
                "required": ["bucket", "key"]
            }
        ),
        Tool(
            name="upload_file",
            description="Upload a file to R2",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "key": {
                        "type": "string",
                        "description": "Object key/path for the uploaded file"
                    },
                    "content": {
                        "type": "string",
                        "description": "File content to upload"
                    },
                    "content_type": {
                        "type": "string",
                        "description": "MIME type of the content (optional, auto-detected if not provided)"
                    },
                    "metadata": {
                        "type": "object",
                        "description": "Optional metadata key-value pairs",
                        "additionalProperties": {"type": "string"}
                    }
                },
                "required": ["bucket", "key", "content"]
            }
        ),
        Tool(
            name="download_file",
            description="Download a file from R2",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "key": {
                        "type": "string",
                        "description": "Object key/path to download"
                    },
                    "as_text": {
                        "type": "boolean",
                        "description": "Whether to return content as text (true) or base64 (false)",
                        "default": True
                    }
                },
                "required": ["bucket", "key"]
            }
        ),
        Tool(
            name="delete_object",
            description="Delete an object from R2",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "key": {
                        "type": "string",
                        "description": "Object key/path to delete"
                    }
                },
                "required": ["bucket", "key"]
            }
        ),
        Tool(
            name="generate_presigned_url",
            description="Generate a presigned URL for R2 object operations",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "key": {
                        "type": "string",
                        "description": "Object key/path"
                    },
                    "operation": {
                        "type": "string",
                        "description": "Operation type",
                        "enum": ["get_object", "put_object", "delete_object"],
                        "default": "get_object"
                    },
                    "expires_in": {
                        "type": "integer",
                        "description": "URL expiration time in seconds",
                        "default": 3600,
                        "minimum": 1,
                        "maximum": 604800
                    }
                },
                "required": ["bucket", "key"]
            }
        ),
        Tool(
            name="search_objects",
            description="Search for objects in R2 bucket by pattern",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket": {
                        "type": "string",
                        "description": "Name of the R2 bucket"
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Search pattern (substring match)"
                    },
                    "max_keys": {
                        "type": "integer",
                        "description": "Maximum number of objects to search",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000
                    }
                },
                "required": ["bucket", "pattern"]
            }
        )
    ]


# Implement tool handlers
@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls for R2 operations."""
    try:
        if name == "list_buckets":
            result = r2_conn.list_buckets()
            return [TextContent(
                type="text",
                text=f"✅ Found {len(result)} R2 buckets:\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "list_objects":
            bucket = arguments["bucket"]
            prefix = arguments.get("prefix", "")
            max_keys = arguments.get("max_keys", 1000)
            
            result = r2_conn.list_objects(bucket, prefix, max_keys)
            return [TextContent(
                type="text",
                text=f"✅ Found {result['count']} objects in bucket '{bucket}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "get_object_metadata":
            bucket = arguments["bucket"]
            key = arguments["key"]
            
            result = r2_conn.get_object_metadata(bucket, key)
            return [TextContent(
                type="text",
                text=f"✅ Metadata for '{bucket}/{key}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "upload_file":
            bucket = arguments["bucket"]
            key = arguments["key"]
            content = arguments["content"]
            content_type = arguments.get("content_type")
            metadata = arguments.get("metadata")
            
            result = r2_conn.upload_file(bucket, key, content, content_type, metadata)
            return [TextContent(
                type="text",
                text=f"✅ Successfully uploaded '{key}' to bucket '{bucket}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "download_file":
            bucket = arguments["bucket"]
            key = arguments["key"]
            as_text = arguments.get("as_text", True)
            
            result = r2_conn.download_file(bucket, key, as_text)
            return [TextContent(
                type="text",
                text=f"✅ Successfully downloaded '{key}' from bucket '{bucket}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "delete_object":
            bucket = arguments["bucket"]
            key = arguments["key"]
            
            result = r2_conn.delete_object(bucket, key)
            return [TextContent(
                type="text",
                text=f"✅ Successfully deleted '{key}' from bucket '{bucket}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "generate_presigned_url":
            bucket = arguments["bucket"]
            key = arguments["key"]
            operation = arguments.get("operation", "get_object")
            expires_in = arguments.get("expires_in", 3600)
            
            result = r2_conn.generate_presigned_url(bucket, key, operation, expires_in)
            return [TextContent(
                type="text",
                text=f"✅ Generated presigned URL for '{bucket}/{key}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        elif name == "search_objects":
            bucket = arguments["bucket"]
            pattern = arguments["pattern"]
            max_keys = arguments.get("max_keys", 100)
            
            result = r2_conn.search_objects(bucket, pattern, max_keys)
            return [TextContent(
                type="text",
                text=f"✅ Search results for pattern '{pattern}' in bucket '{bucket}':\n\n" + 
                     json.dumps(result, indent=2, default=str)
            )]
        
        else:
            raise ValueError(f"Unknown tool: {name}")
    
    except Exception as e:
        error_msg = f"❌ Error executing {name}: {str(e)}"
        logger.error(error_msg)
        return [TextContent(type="text", text=error_msg)]


# Main server runner
async def main():
    """Main server entry point."""
    logger.info("Starting R2 MCP Server...")
    
    # Verify R2 connection on startup
    try:
        buckets = r2_conn.list_buckets()
        logger.info(f"✅ R2 connection verified. Found {len(buckets)} buckets.")
    except Exception as e:
        logger.error(f"❌ Failed to connect to R2: {e}")
        sys.exit(1)
    
    # Run the server
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main()) 