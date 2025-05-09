"""
Document utilities for the CAR MCP server.

This module provides utilities for working with code documents, including ID generation
and chunking functions.
"""

import hashlib
from typing import Dict, List, Any

def generate_document_id(content: str, metadata: Dict[str, Any] = None) -> str:
    """
    Generate a unique document ID based on content and metadata.
    
    Args:
        content: Document content
        metadata: Optional metadata to include in the ID generation
        
    Returns:
        A unique ID string
    """
    # Create a string combining content and key metadata
    id_base = content
    if metadata:
        if 'file_path' in metadata:
            id_base += metadata['file_path']
        if 'language' in metadata:
            id_base += metadata['language']
    
    # Generate SHA-256 hash and take first 16 characters
    return hashlib.sha256(id_base.encode('utf-8')).hexdigest()[:16]

def chunk_code(
    code: str, 
    max_chunk_size: int = 1000, 
    overlap: int = 100
) -> List[str]:
    """
    Split code into chunks with overlap for better context preservation.
    
    Args:
        code: Code to split into chunks
        max_chunk_size: Maximum size of each chunk
        overlap: Overlap between chunks
        
    Returns:
        List of code chunks
    """
    if len(code) <= max_chunk_size:
        return [code]
    
    lines = code.split('\n')
    chunks = []
    current_chunk = []
    current_size = 0
    
    for line in lines:
        line_size = len(line) + 1  # +1 for newline
        
        # If this line would exceed max size, store current chunk and start new one
        if current_size + line_size > max_chunk_size and current_chunk:
            chunks.append('\n'.join(current_chunk))
            
            # Keep overlap lines for context
            overlap_lines = current_chunk[-overlap//20:] if overlap > 0 else []
            current_chunk = overlap_lines
            current_size = sum(len(line) + 1 for line in overlap_lines)
        
        current_chunk.append(line)
        current_size += line_size
    
    # Add the last chunk if not empty
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    return chunks