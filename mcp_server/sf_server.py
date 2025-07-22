"""
Reference:
https://adrianleexinhan.medium.com/unlocking-ai-potential-connecting-snowflake-to-claude-with-mcp-in-cursor-034fa7b9a783

Snowflake Model Context Protocol (MCP) Server

This server enables Claude to execute SQL queries on Snowflake databases through
the Model Context Protocol (MCP). It handles connection lifecycle management,
query execution, and result formatting.
"""
import os
import asyncio
import logging
import json
import time
from typing import Optional, Any, Dict, List
import sys

import snowflake.connector
from dotenv import load_dotenv
import mcp.server.stdio
from mcp.server import Server
from mcp.types import Tool, TextContent


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('snowflake_server')

if len(sys.argv) > 1:
    log_path = sys.argv[1]
else:
    log_path = 'snowflake_mcp_server.log'
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

load_dotenv()


class SnowflakeConnection:
    """Manages Snowflake database connections and query execution."""
    
    def __init__(self) -> None:
        """Initialize Snowflake connection configuration from environment variables."""
        self.config = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        }
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None
        
        # Log configuration (excluding password)
        safe_config = {k: v for k, v in self.config.items() if k != 'password'}
        logger.info(f"Initialized with config: {json.dumps(safe_config)}")
    
    def verify_link(self) -> snowflake.connector.SnowflakeConnection:
        """
        Ensure the database connection is available and valid.
        
        Returns:
            A valid Snowflake connection
            
        Raises:
            Exception: If connection cannot be established
        """
        try:
            # Create new connection if needed
            if self.conn is None:
                logger.info("Creating new Snowflake connection...")
                self.conn = snowflake.connector.connect(
                    **self.config,
                    client_session_keep_alive=True,
                    network_timeout=150,
                    login_timeout=150
                )
                self.conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                logger.info("New connection established and configured")
            
            # Test if connection is valid
            try:
                self.conn.cursor().execute("SELECT 1")
            except Exception as e:
                logger.error(f"Connection error: {str(e)}")
                logger.info("Connection lost, reconnecting...")
                self.conn = None
                return self.verify_link()
                
            return self.conn
            
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            raise
    
    def process_request(self, command: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return the results.
        
        Args:
            command: SQL query to execute
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            Exception: If query execution fails
        """
        start_time = time.time()
        command_preview = command[:200] + "..." if len(command) > 200 else command
        logger.info(f"Executing query: {command_preview}")
        
        try:
            conn = self.verify_link()
            
            # Determine if this is a data modification query
            is_write_operation = any(
                command.strip().upper().startswith(word) 
                for word in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
            )
            
            with conn.cursor() as cursor:
                if is_write_operation:
                    # Use transaction for write operations
                    cursor.execute("BEGIN")
                    try:
                        cursor.execute(command)
                        conn.commit()
                        execution_time = time.time() - start_time
                        logger.info(f"Write query executed in {execution_time:.2f}s")
                        return [{"affected_rows": cursor.rowcount}]
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error executing write query: {str(e)}")
                        raise
                else:
                    # Read operations
                    cursor.execute(command)
                    execution_time = time.time() - start_time
                    
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        results = [dict(zip(columns, row)) for row in rows]
                        logger.info(f"Read query returned {len(results)} rows in {execution_time:.2f}s")
                        return results
                    
                    logger.info(f"Read query executed in {execution_time:.2f}s (no results)")
                    return []
                    
        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"SQL Error: {str(e)}")
            logger.error(f"Error Code: {getattr(e, 'errno', 'unknown')}")
            raise
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise
    
    def cleanup(self) -> None:
        """Safely close the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.conn = None


class SnowflakeServer(Server):
    """MCP server that handles Snowflake database operations."""
    
    def __init__(self) -> None:
        """Initialize the MCP server with a Snowflake connection."""
        super().__init__(name="snowflake-server")
        self.db = SnowflakeConnection()
        logger.info("SnowflakeServer initialized")

        @self.list_tools()
        async def get_supported_operations():
            """Return list of available tools."""
            return [
                Tool(
                    name="execute_query",
                    description="Execute a SQL query on Snowflake",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            }
                        },
                        "required": ["query"]
                    }
                )
            ]

        @self.call_tool()
        async def handle_operation(name: str, arguments: Dict[str, Any]):
            """
            Handle tool call requests.
            
            Args:
                name: Tool name
                arguments: Tool arguments
                
            Returns:
                List of TextContent objects with execution results
            """
            if name == "execute_query":
                start_time = time.time()
                try:
                    result = self.db.process_request(arguments["query"])
                    execution_time = time.time() - start_time
                    
                    return [TextContent(
                        type="text",
                        text=f"Results (execution time: {execution_time:.2f}s):\n{result}"
                    )]
                except Exception as e:
                    error_message = f"Error executing query: {str(e)}"
                    logger.error(error_message)
                    return [TextContent(
                        type="text",
                        text=error_message
                    )]

    def __del__(self) -> None:
        """Clean up resources when the server is deleted."""
        if hasattr(self, 'db'):
            self.db.cleanup()


async def start_service() -> None:
    """Start and run the MCP server."""
    try:
        # Initialize the server
        server = SnowflakeServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting server")
        
        # Run the server using stdio communication
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                initialization_options
            )
    except Exception as e:
        logger.critical(f"Server failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Server shutting down")


if __name__ == "__main__":
    asyncio.run(start_service())