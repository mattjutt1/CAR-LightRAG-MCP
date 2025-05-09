"""
Management tools for the CAR MCP server.

This module provides tools for managing the code knowledge base,
including listing statistics and deleting documents.
"""

import logging
from typing import Any, Dict, List, Optional

from fastmcp import Tool

from ...features.memory_services.client_service import MemoryClient # To be MemoryService

# Configure logging
logger = logging.getLogger("car_mcp.mcp_interface.tool_adapters.management_tool_adapter")

def list_code_statistics_tool(memory_client: MemoryClient) -> Tool:
    """
    Create a tool for listing statistics about the code knowledge base.
    
    Args:
        memory_client: The memory client to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # This is a simplified implementation - in a real server,
            # you would want to implement methods in MemoryClient to get these statistics
            collection = memory_client.collection
            count = collection.count()
            
            # Get language statistics - this would ideally be a method in MemoryClient
            languages = {}
            languages_results = collection.get(
                where={"type": {"$in": ["code_file", "code_block", "code_snippet"]}}
            )
            
            if languages_results and "metadatas" in languages_results:
                for metadata in languages_results["metadatas"]:
                    if "language" in metadata:
                        lang = metadata["language"]
                        languages[lang] = languages.get(lang, 0) + 1
            
            return {
                "success": True,
                "message": f"Code knowledge base contains {count} documents",
                "statistics": {
                    "total_documents": count,
                    "languages": languages
                }
            }
            
        except Exception as e:
            error_msg = f"Error listing code statistics: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="list_code_statistics",
        description="List statistics about the code knowledge base",
        input_schema={
            "type": "object",
            "properties": {}
        },
        execute=execute
    )

def delete_code_document_tool(memory_client: MemoryClient) -> Tool:
    """
    Create a tool for deleting a code document from the knowledge base.
    
    Args:
        memory_client: The memory client to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            document_id = params.get("document_id")
            
            if not document_id:
                return {"error": "document_id parameter is required"}
            
            # Check if document exists
            document = memory_client.get_document(document_id)
            if not document:
                return {"error": f"Document with ID {document_id} not found"}
            
            # Delete the document
            memory_client.delete_document(document_id)
            
            return {
                "success": True,
                "message": f"Deleted document with ID {document_id}"
            }
            
        except Exception as e:
            error_msg = f"Error deleting code document: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="delete_code_document",
        description="Delete a code document from the knowledge base",
        input_schema={
            "type": "object",
            "properties": {
                "document_id": {
                    "type": "string",
                    "description": "ID of the document to delete"
                }
            },
            "required": ["document_id"]
        },
        execute=execute
    )

def delete_by_path_tool(memory_client: MemoryClient) -> Tool:
    """
    Create a tool for deleting code documents by file path.
    
    Args:
        memory_client: The memory client to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            file_path = params.get("file_path")
            
            if not file_path:
                return {"error": "file_path parameter is required"}
            
            # This is a simplified implementation - in a real server,
            # you would want to implement a method in MemoryClient for this
            collection = memory_client.collection
            results = collection.get(
                where={"file_path": file_path}
            )
            
            if not results or not results["ids"]:
                return {"error": f"No documents found with file_path {file_path}"}
            
            # Delete each document
            for doc_id in results["ids"]:
                memory_client.delete_document(doc_id)
            
            return {
                "success": True,
                "message": f"Deleted {len(results['ids'])} documents for file path {file_path}"
            }
            
        except Exception as e:
            error_msg = f"Error deleting by path: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="delete_by_path",
        description="Delete code documents by file path",
        input_schema={
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path of the file to delete documents for"
                }
            },
            "required": ["file_path"]
        },
        execute=execute
    )