"""
Search tools for the CAR MCP server.

This module provides tools for searching the code knowledge base
using various criteria and methods.
"""

import logging
from typing import Any, Dict, List, Optional

from fastmcp import Tool

from ...features.memory_services.client_service import MemoryClient # To be MemoryService
from ...features.context_logging.logger_service import ContextLogger # To be ContextLoggingService

# Configure logging
logger = logging.getLogger("car_mcp.mcp_interface.tool_adapters.search_tool_adapter")

def search_code_knowledge_tool(
    memory_client: MemoryClient,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for searching the code knowledge base.
    
    Args:
        memory_client: The memory client to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            query = params.get("query")
            limit = params.get("limit", 5)
            filters = params.get("filters")
            notebook_path = params.get("log_notebook")
            
            if not query:
                return {"error": "query parameter is required"}
            
            # Search for matches
            results = memory_client.search(
                query=query,
                limit=limit,
                filters=filters
            )
            
            # Log the search if a notebook is provided
            if notebook_path:
                context_logger.log_code_interaction(
                    notebook_path=notebook_path,
                    query=query,
                    results=results,
                    metadata={"filters": filters, "limit": limit}
                )
            
            return {
                "success": True,
                "message": f"Found {len(results)} results for query: {query}",
                "results": results
            }
            
        except Exception as e:
            error_msg = f"Error searching code knowledge: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="search_code_knowledge",
        description="Search the code knowledge base",
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results to return"
                },
                "filters": {
                    "type": "object",
                    "description": "Filters to apply to the search"
                },
                "log_notebook": {
                    "type": "string",
                    "description": "Path to a log notebook (optional)"
                }
            },
            "required": ["query"]
        },
        execute=execute
    )

def search_by_language_tool(
    memory_client: MemoryClient,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for searching the code knowledge base by language.
    
    Args:
        memory_client: The memory client to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            query = params.get("query")
            language = params.get("language")
            limit = params.get("limit", 5)
            notebook_path = params.get("log_notebook")
            
            if not query:
                return {"error": "query parameter is required"}
            
            if not language:
                return {"error": "language parameter is required"}
            
            # Create filter for the language
            filters = {"language": language}
            
            # Search for matches
            results = memory_client.search(
                query=query,
                limit=limit,
                filters=filters
            )
            
            # Log the search if a notebook is provided
            if notebook_path:
                context_logger.log_code_interaction(
                    notebook_path=notebook_path,
                    query=f"[{language}] {query}",
                    results=results,
                    metadata={"language": language, "limit": limit}
                )
            
            return {
                "success": True,
                "message": f"Found {len(results)} {language} results for query: {query}",
                "results": results
            }
            
        except Exception as e:
            error_msg = f"Error searching by language: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="search_by_language",
        description="Search the code knowledge base by programming language",
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                },
                "language": {
                    "type": "string",
                    "description": "The programming language to filter by"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results to return"
                },
                "log_notebook": {
                    "type": "string",
                    "description": "Path to a log notebook (optional)"
                }
            },
            "required": ["query", "language"]
        },
        execute=execute
    )

def similar_code_tool(
    memory_client: MemoryClient,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for finding similar code to a given code snippet.
    
    Args:
        memory_client: The memory client to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            code = params.get("code")
            language = params.get("language")
            limit = params.get("limit", 5)
            notebook_path = params.get("log_notebook")
            
            if not code:
                return {"error": "code parameter is required"}
            
            # Create language filter if provided
            filters = {"language": language} if language else None
            
            # Search for similar code
            results = memory_client.search(
                query=code,
                limit=limit,
                filters=filters
            )
            
            # Log the search if a notebook is provided
            if notebook_path:
                context_logger.log_code_interaction(
                    notebook_path=notebook_path,
                    query="[SIMILAR CODE SEARCH]",
                    results=results,
                    metadata={
                        "code_sample": code[:100] + ("..." if len(code) > 100 else ""),
                        "language": language,
                        "limit": limit
                    }
                )
            
            return {
                "success": True,
                "message": f"Found {len(results)} similar code snippets",
                "results": results
            }
            
        except Exception as e:
            error_msg = f"Error finding similar code: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="similar_code",
        description="Find similar code to a given code snippet",
        input_schema={
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The code snippet to find similar code for"
                },
                "language": {
                    "type": "string",
                    "description": "The programming language to filter by (optional)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results to return"
                },
                "log_notebook": {
                    "type": "string",
                    "description": "Path to a log notebook (optional)"
                }
            },
            "required": ["code"]
        },
        execute=execute
    )