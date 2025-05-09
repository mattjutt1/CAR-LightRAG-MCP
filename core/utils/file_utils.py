"""
File utilities for the CAR MCP server.

This module provides utilities for safely working with files, including
binary detection and safe file reading.
"""

import os
from typing import Optional
from car_mcp.core.config import config

# Constants
# FILE_READ_CHUNK_SIZE_BYTES: int = 1024 # Moved to config

def is_binary_file(file_path: str) -> bool:
    """
    Check if a file is binary or text.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if the file is binary, False otherwise
    """
    # Check file extension first for common binary types
    # extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.gz',
    #              '.exe', '.bin', '.pyc', '.class', '.jar', '.war', '.ear'] # Moved to config
    
    if any(file_path.lower().endswith(ext) for ext in config.binary_file_extensions):
        return True
    
    # Read a chunk of the file to test for binary content
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f.read(config.file_read_chunk_size_bytes) # Use config value
        return False
    except UnicodeDecodeError:
        return True

def safe_read_file(file_path: str, max_size: Optional[int] = None) -> str:
    """
    Safely read a file with size limits.
    
    Args:
        file_path: Path to the file
        max_size: Maximum size to read in bytes. Defaults to AppConfig.max_file_read_size_bytes.
        
    Returns:
        File content as text
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if is_binary_file(file_path):
        raise ValueError(f"Cannot read binary file: {file_path}")

    current_max_size = max_size if max_size is not None else config.max_file_read_size_bytes
    
    # Check file size
    file_size = os.path.getsize(file_path)
    if file_size > current_max_size:
        raise ValueError(f"File too large ({file_size} bytes > {current_max_size} bytes): {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()