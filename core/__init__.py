"""
Core package for the CAR/MCP server.

This package contains shared, foundational components including:
- Custom exceptions (`core.exceptions`)
- Generic utilities (`core.utils`)
- Common data models (`core.common_models`)
- DI protocols/interfaces (`core.protocols`)
- Configuration management (`core.config`)
"""

# Re-export key components for easier access
from .config import AppConfig
from .exceptions import (
    CARException,
    MemoryError,
    DocumentProcessingError,
    CodeExtractionError,
    VectorStoreError,
    SearchError,
    ContextLoggerError,
    ToolExecutionError,
    ServerConfigurationError,
    KnowledgeGraphError,
    EntityNotFoundError,
    DatabaseError, # Added new exception
)
# common_models and protocols are currently placeholders, add exports when implemented
# from .common_models import ...
# from .protocols import ...
from . import utils

__all__ = [
    "AppConfig",
    "CARException",
    "MemoryError",
    "DocumentProcessingError",
    "CodeExtractionError",
    "VectorStoreError",
    "SearchError",
    "ContextLoggerError",
    "ToolExecutionError",
    "ServerConfigurationError",
    "KnowledgeGraphError",
    "EntityNotFoundError",
    "DatabaseError", # Added new exception
    "utils",
]

# Or, allow direct imports from submodules:
# import car_mcp.core.config
# import car_mcp.core.exceptions
# import car_mcp.core.protocols
# import car_mcp.core.utils
# import car_mcp.core.common_models