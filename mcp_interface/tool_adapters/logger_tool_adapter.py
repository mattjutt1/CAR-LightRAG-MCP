"""
Context logger tools for the CAR MCP server.

This module provides tools for creating and managing log notebooks.
"""

import logging
from typing import Any, Dict, List, Optional

from fastmcp import Tool

from ...features.context_logging.logger_service import ContextLogger # To be ContextLoggingService

# Configure logging
logger = logging.getLogger("car_mcp.mcp_interface.tool_adapters.logger_tool_adapter")

def create_log_notebook_tool(context_logger: ContextLogger) -> Tool:
    """
    Create a tool for creating a new log notebook.
    
    Args:
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            name = params.get("name", "CAR Log")
            
            # Create a new log notebook
            notebook_path = context_logger.create_log_notebook(name)
            
            return {
                "success": True,
                "message": f"Created log notebook: {notebook_path}",
                "notebook_path": notebook_path
            }
            
        except Exception as e:
            error_msg = f"Error creating log notebook: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="create_log_notebook",
        description="Create a new log notebook",
        input_schema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Name for the log notebook"
                }
            }
        },
        execute=execute
    )

def list_log_notebooks_tool(context_logger: ContextLogger) -> Tool:
    """
    Create a tool for listing log notebooks.
    
    Args:
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # List all log notebooks
            notebooks = context_logger.list_notebooks()
            
            return {
                "success": True,
                "message": f"Found {len(notebooks)} log notebooks",
                "notebooks": notebooks
            }
            
        except Exception as e:
            error_msg = f"Error listing log notebooks: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="list_log_notebooks",
        description="List all log notebooks",
        input_schema={
            "type": "object",
            "properties": {}
        },
        execute=execute
    )