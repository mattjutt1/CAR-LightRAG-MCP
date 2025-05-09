"""
Exception classes for the CAR MCP server.

This module defines various exception classes used throughout the CAR MCP server.
"""

class CARException(Exception):
    """Base exception class for all CAR MCP server exceptions."""
    pass

class MemoryError(CARException):
    """Exception raised for errors related to the memory system."""
    pass

class DocumentProcessingError(CARException):
    """Exception raised for errors during document processing."""
    pass

class CodeExtractionError(DocumentProcessingError):
    """Exception raised when code extraction from a document fails."""
    pass

class VectorStoreError(MemoryError):
    """Exception raised for errors related to the vector store operations."""
    pass

class SearchError(MemoryError):
    """Exception raised when a search operation fails."""
    pass

class ContextLoggerError(CARException):
    """Exception raised for errors in the context logger."""
    pass

class ToolExecutionError(CARException):
    """Exception raised when a tool execution fails."""
    pass

class ServerConfigurationError(CARException):
    """Exception raised for server configuration issues."""
    pass

class KnowledgeGraphError(CARException):
    """Base exception for knowledge graph operations."""
    pass

class EntityNotFoundError(KnowledgeGraphError):
    """Exception raised when an entity is not found."""
    pass

class DatabaseError(CARException):
    """Exception raised for general database operations errors."""
    pass