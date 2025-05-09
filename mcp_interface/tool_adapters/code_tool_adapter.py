"""
Code knowledge addition tools for the CAR MCP server.

This module provides tools for adding code files, markdown text containing code,
and individual code snippets to the knowledge base.
"""

import os
import logging
from typing import Any, Dict, List, Optional

from fastmcp import Tool

from ...features.memory_services.client_service import MemoryClient # To be MemoryService
from ...features.document_processing.processor_service import DocumentProcessor # To be DocumentProcessingService
from ...features.context_logging.logger_service import ContextLogger # To be ContextLoggingService
from ...features.document_processing.utils.document_utils import generate_document_id

# Configure logging
logger = logging.getLogger("car_mcp.mcp_interface.tool_adapters.code_tool_adapter")

def add_code_file_tool(
    memory_client: MemoryClient,
    document_processor: DocumentProcessor,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for adding a code file to the knowledge base.
    
    Args:
        memory_client: The memory client to use
        document_processor: The document processor to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            file_path = params.get("file_path")
            notebook_path = params.get("log_notebook")
            
            if not file_path:
                return {"error": "file_path parameter is required"}
            
            if not os.path.exists(file_path):
                return {"error": f"File not found: {file_path}"}
            
            # Process the file
            documents = document_processor.process_file(file_path)
            
            if not documents:
                return {"error": f"No code content extracted from {file_path}"}
            
            # Add documents to memory
            document_ids = []
            for doc in documents:
                document_id = memory_client.add_document(
                    document_id=doc["id"],
                    content=doc["content"],
                    metadata=doc["metadata"]
                )
                document_ids.append(document_id)
            
            # Log the operation if a notebook is provided
            if notebook_path:
                context_logger.log_memory_operation(
                    notebook_path=notebook_path,
                    operation="add_code_file",
                    documents=documents,
                    metadata={"file_path": file_path}
                )
            
            return {
                "success": True,
                "message": f"Added {len(documents)} code chunks from {file_path}",
                "document_ids": document_ids
            }
            
        except Exception as e:
            error_msg = f"Error adding code file: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="add_code_file",
        description="Add a code file to the knowledge base",
        input_schema={
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the code file"
                },
                "log_notebook": {
                    "type": "string",
                    "description": "Path to a log notebook (optional)"
                }
            },
            "required": ["file_path"]
        },
        execute=execute
    )

def add_code_text_tool(
    memory_client: MemoryClient,
    document_processor: DocumentProcessor,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for adding code from markdown text to the knowledge base.
    
    Args:
        memory_client: The memory client to use
        document_processor: The document processor to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            text = params.get("text")
            source = params.get("source", "Unknown")
            notebook_path = params.get("log_notebook")
            
            if not text:
                return {"error": "text parameter is required"}
            
            # Process the markdown text
            source_metadata = {"source": source}
            documents = document_processor.process_markdown_text(text, source_metadata)
            
            if not documents:
                return {"error": "No code blocks found in the text"}
            
            # Add documents to memory
            document_ids = []
            for doc in documents:
                document_id = memory_client.add_document(
                    document_id=doc["id"],
                    content=doc["content"],
                    metadata=doc["metadata"]
                )
                document_ids.append(document_id)
            
            # Log the operation if a notebook is provided
            if notebook_path:
                context_logger.log_memory_operation(
                    notebook_path=notebook_path,
                    operation="add_code_text",
                    documents=documents,
                    metadata={"source": source}
                )
            
            return {
                "success": True,
                "message": f"Added {len(documents)} code blocks from text",
                "document_ids": document_ids
            }
            
        except Exception as e:
            error_msg = f"Error adding code text: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="add_code_text",
        description="Add code blocks from markdown text to the knowledge base",
        input_schema={
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Markdown text containing code blocks"
                },
                "source": {
                    "type": "string",
                    "description": "Source of the text (e.g., 'Documentation', 'Chat')"
                },
                "log_notebook": {
                    "type": "string",
                    "description": "Path to a log notebook (optional)"
                }
            },
            "required": ["text"]
        },
        execute=execute
    )

def add_code_snippet_tool(
    memory_client: MemoryClient,
    document_processor: DocumentProcessor,
    context_logger: ContextLogger
) -> Tool:
    """
    Create a tool for adding a single code snippet to the knowledge base.
    
    Args:
        memory_client: The memory client to use
        document_processor: The document processor to use
        context_logger: The context logger to use
        
    Returns:
        The tool definition
    """
    def execute(params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            code = params.get("code")
            language = params.get("language", "text")
            description = params.get("description", "")
            source = params.get("source", "Unknown")
            notebook_path = params.get("log_notebook")
            
            if not code:
                return {"error": "code parameter is required"}
            
            # Create metadata
            metadata = {
                "language": language,
                "description": description,
                "source": source,
                "type": "code_snippet"
            }
            
            # Generate ID
            document_id = generate_document_id(code, metadata)
            
            # Add to memory
            memory_client.add_document(
                document_id=document_id,
                content=code,
                metadata=metadata
            )
            
            document = {
                "id": document_id,
                "content": code,
                "metadata": metadata
            }
            
            # Log the operation if a notebook is provided
            if notebook_path:
                context_logger.log_memory_operation(
                    notebook_path=notebook_path,
                    operation="add_code_snippet",
                    documents=[document],
                    metadata={"source": source}
                )
            
            return {
                "success": True,
                "message": f"Added code snippet",
                "document_id": document_id
            }
            
        except Exception as e:
            error_msg = f"Error adding code snippet: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
    
    return Tool(
        name="add_code_snippet",
        description="Add a single code snippet to the knowledge base",
        input_schema={
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The code snippet"
                },
                "language": {
                    "type": "string",
                    "description": "The programming language of the code"
                },
                "description": {
                    "type": "string",
                    "description": "A description of the code snippet"
                },
                "source": {
                    "type": "string",
                    "description": "Source of the code (e.g., 'User', 'Project')"
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