"""
Main server implementation for the CAR MCP server.

This module implements the CARServer class which integrates with FastMCP
to provide code understanding, storage, and retrieval capabilities.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union

try:
    from fastmcp import Server, Resource, Tool, register_tools as fastmcp_register_tools
except ImportError:
    raise ImportError("FastMCP is required. Please install it using: pip install fastmcp")

from ..features.memory_services.client_service import MemoryClient # Will become MemoryService
# The 'register_tools' import will need significant changes later.
# For now, commenting out as its source 'tools.py' will be restructured.
# from .tools import register_tools
from ..features.document_processing.processor_service import DocumentProcessor # Will become DocumentProcessingService
from ..features.context_logging.logger_service import ContextLogger # Will become ContextLoggingService
from ..core.exceptions import CARException, ServerConfigurationError # Assuming these are in core.exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("car_mcp.mcp_interface.server_app")

class CARServer:
    """
    Main server for Code Augmented Reasoning (CAR) MCP implementation.
    
    This server provides tools and resources for code understanding, storage, 
    and retrieval to enhance Claude's code interactions.
    """
    
    def __init__(
        self, 
        name: str = "car-server",
        host: str = "localhost", 
        port: int = 8000, 
        vector_db_path: Optional[str] = None,
        log_notebook_path: Optional[str] = None
    ):
        """
        Initialize the CAR MCP server.
        
        Args:
            name: The name of the server
            host: The host to bind the server to
            port: The port to bind the server to
            vector_db_path: Path to the vector database
            log_notebook_path: Path to store context logger notebooks
        """
        self.name = name
        self.host = host
        self.port = port
        
        # Set default paths if not provided
        self.vector_db_path = vector_db_path or os.path.join(os.getcwd(), "car_vector_db")
        self.log_notebook_path = log_notebook_path or os.path.join(os.getcwd(), "car_logs")
        
        # Initialize components
        self.memory_client = MemoryClient(self.vector_db_path)
        self.document_processor = DocumentProcessor()
        self.context_logger = ContextLogger(self.log_notebook_path)
        
        # Initialize server
        self.server = Server(name=self.name)
        
        # Register tools and resources
        self._register_tools_and_resources()
        
        logger.info(f"CAR MCP Server initialized with vector DB at {self.vector_db_path}")
        logger.info(f"Context logs will be stored at {self.log_notebook_path}")
    
    def _register_tools_and_resources(self) -> None:
        """Register all tools and resources with the FastMCP server."""
        # Register tools from the tools module
        tools = register_tools(self.memory_client, self.document_processor, self.context_logger)
        for tool in tools:
            self.server.register_tool(tool)
        
        # Register code knowledge resource
        self.server.register_resource(
            Resource(
                uri="car://code-knowledge",
                description="Access to the code knowledge base",
                handler=self._handle_code_knowledge_resource
            )
        )
        
        logger.info(f"Registered {len(tools)} tools and 1 resource")
    
    def _handle_code_knowledge_resource(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle requests for the code knowledge resource."""
        query = params.get("query")
        limit = params.get("limit", 5)
        
        if not query:
            return {"error": "Query parameter is required"}
        
        try:
            results = self.memory_client.search(query, limit=limit)
            return {"results": results}
        except Exception as e:
            logger.error(f"Error handling code knowledge resource: {str(e)}")
            return {"error": str(e)}
    
    def start(self) -> None:
        """Start the CAR MCP server."""
        try:
            self.server.start(host=self.host, port=self.port)
            logger.info(f"CAR MCP Server started at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to start CAR MCP Server: {str(e)}")
            raise ServerConfigurationError(f"Failed to start server: {str(e)}")
    
    def stop(self) -> None:
        """Stop the CAR MCP server."""
        try:
            self.server.stop()
            logger.info("CAR MCP Server stopped")
        except Exception as e:
            logger.error(f"Error stopping CAR MCP Server: {str(e)}")