"""
Main document processor implementation for the CAR MCP server.

This module implements the DocumentProcessor class for processing code documents,
including parsing, tokenization, and extracting useful information from code files.
"""

import os
import logging
from typing import Any, Dict, List, Optional

from ...core.utils.file_utils import safe_read_file
from .utils import ( # Assuming utils is a sub-package in the current directory
    extract_code_blocks,
    detect_language_from_file,
    detect_language_from_content,
    generate_document_id,
    chunk_code
)
from ...core.exceptions import DocumentProcessingError, CodeExtractionError # Assuming these are moved to core
from .metadata_extractors import (
    extract_python_metadata,
    extract_js_ts_metadata,
    extract_java_metadata,
    extract_go_metadata,
    extract_c_family_metadata
)

# Configure logging
logger = logging.getLogger("car_mcp.features.document_processing.processor_service")

class DocumentProcessor:
    """
    Processes code documents for storage and retrieval.
    
    Handles parsing, tokenization, and extraction of useful information
    from code files and text containing code.
    """
    
    def __init__(
        self, 
        max_chunk_size: int = 1000,
        chunk_overlap: int = 100
    ):
        """
        Initialize the document processor.
        
        Args:
            max_chunk_size: Maximum size of each code chunk
            chunk_overlap: Overlap between chunks for better context
        """
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        logger.info("DocumentProcessor initialized")
    
    def process_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Process a code file and prepare it for storage.
        
        Args:
            file_path: Path to the code file
            
        Returns:
            List of processed document chunks with metadata
        """
        try:
            # Read the file
            content = safe_read_file(file_path)
            
            # Detect language
            language = detect_language_from_file(file_path)
            
            # Create document metadata
            metadata = {
                "file_path": file_path,
                "language": language,
                "type": "code_file",
                "file_name": os.path.basename(file_path)
            }
            
            # Extract additional metadata based on language
            code_metadata = self._extract_code_metadata(content, language)
            metadata.update(code_metadata)
            
            # Chunk the code if necessary
            chunks = chunk_code(content, self.max_chunk_size, self.chunk_overlap)
            
            # Create document objects for each chunk
            documents = []
            for i, chunk in enumerate(chunks):
                # Generate unique ID for the chunk
                chunk_metadata = dict(metadata)
                chunk_metadata.update({
                    "chunk_index": i,
                    "total_chunks": len(chunks)
                })
                
                document_id = generate_document_id(chunk, chunk_metadata)
                
                documents.append({
                    "id": document_id,
                    "content": chunk,
                    "metadata": chunk_metadata
                })
            
            logger.info(f"Processed file {file_path} into {len(documents)} chunks")
            return documents
            
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise DocumentProcessingError(error_msg)
    
    def process_markdown_text(self, text: str, source_metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Process markdown text containing code blocks.
        
        Args:
            text: Markdown text containing code blocks
            source_metadata: Optional additional metadata about the source
            
        Returns:
            List of processed document chunks with metadata
        """
        try:
            # Extract code blocks
            code_blocks = extract_code_blocks(text)
            
            if not code_blocks:
                logger.warning("No code blocks found in markdown text")
                return []
            
            # Create document objects for each code block
            documents = []
            for i, block in enumerate(code_blocks):
                # Create metadata for the code block
                metadata = {
                    "language": block["language"],
                    "type": "code_block",
                    "block_index": i,
                    "total_blocks": len(code_blocks)
                }
                
                # Add source metadata if provided
                if source_metadata:
                    metadata.update(source_metadata)
                
                # Extract additional metadata based on language
                code_metadata = self._extract_code_metadata(block["code"], block["language"])
                metadata.update(code_metadata)
                
                # Generate unique ID for the code block
                document_id = generate_document_id(block["code"], metadata)
                
                documents.append({
                    "id": document_id,
                    "content": block["code"],
                    "metadata": metadata
                })
            
            logger.info(f"Processed markdown text into {len(documents)} code blocks")
            return documents
            
        except Exception as e:
            error_msg = f"Error processing markdown text: {str(e)}"
            logger.error(error_msg)
            raise CodeExtractionError(error_msg)
    
    def _extract_code_metadata(self, code: str, language: str) -> Dict[str, Any]:
        """
        Extract metadata from code content based on language.
        
        Args:
            code: Code content
            language: Programming language
            
        Returns:
            Dictionary of extracted metadata
        """
        metadata = {}
        
        # Calculate basic metrics
        lines = code.split("\n")
        metadata["line_count"] = len(lines)
        metadata["character_count"] = len(code)
        
        # Language-specific metadata extraction
        if language == "python":
            metadata.update(extract_python_metadata(code))
        elif language in ["javascript", "typescript", "jsx", "tsx"]:
            metadata.update(extract_js_ts_metadata(code))
        elif language in ["java", "kotlin"]:
            metadata.update(extract_java_metadata(code))
        elif language == "go":
            metadata.update(extract_go_metadata(code))
        elif language in ["c", "cpp", "csharp"]:
            metadata.update(extract_c_family_metadata(code))
        
        return metadata