"""
Core configuration management.

This module is responsible for loading and providing access to application
configurations, such as database connection strings, API keys, feature flags,
and other operational parameters.

Configurations might be loaded from environment variables, .env files,
or other configuration sources.
"""
import os
from typing import List

# Default values, can be overridden by environment variables or other sources
DEFAULT_BINARY_FILE_EXTENSIONS: List[str] = [
    '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.gz',
    '.exe', '.dll', '.so', '.dylib', '.bin', '.pyc', '.class',
    '.jar', '.war', '.ear', '.o', '.a', '.obj', '.lib',
    '.mp3', '.wav', '.ogg', '.mp4', '.mov', '.avi', '.mkv',
    '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
    '.iso', '.img', '.dmg', '.sqlite', '.db'
]
DEFAULT_MAX_FILE_READ_SIZE_BYTES: int = 1 * 1024 * 1024  # 1 MB
DEFAULT_FILE_READ_CHUNK_SIZE_BYTES: int = 1024 # For binary detection

class AppConfig:
    """
    Application configuration settings.
    Values are typically loaded from environment variables or a configuration file.
    """
    def __init__(self):
        self.database_url: str = os.getenv("DATABASE_URL", "sqlite:///./default.db")
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
        
        binary_extensions_str = os.getenv("BINARY_FILE_EXTENSIONS")
        self.binary_file_extensions: List[str] = (
            binary_extensions_str.split(',') if binary_extensions_str
            else DEFAULT_BINARY_FILE_EXTENSIONS
        )
        
        self.max_file_read_size_bytes: int = int(
            os.getenv("MAX_FILE_READ_SIZE_BYTES", DEFAULT_MAX_FILE_READ_SIZE_BYTES)
        )
        
        self.file_read_chunk_size_bytes: int = int(
            os.getenv("FILE_READ_CHUNK_SIZE_BYTES", DEFAULT_FILE_READ_CHUNK_SIZE_BYTES)
        )
        # Add other configurations here

# Global config instance.
# In a larger application, you might prefer dependency injection for AppConfig.
config = AppConfig()