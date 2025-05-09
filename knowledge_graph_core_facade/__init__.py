"""
Knowledge Graph Core Facade package.

This package provides a modular architecture for interacting with the knowledge graph
through a facade pattern. It maintains backward compatibility while organizing
functionality into focused modules.

The main components are:
- KnowledgeGraph: The facade class providing a unified API
- Entity, Observation, Relation: Core model classes
- Utility functions: For caching, serialization, and database operations
"""

# Main facade class
from .graph_facade import KnowledgeGraph

# Core model classes
from .kg_models_all import Entity, Observation, Relation

# Database utilities
from .db_handler import init_database, get_connection

# Utility functions
from .kg_utils import (
    get_cache_key,
    invalidate_cache,
    execute_with_retry,
    serialize_embedding,
    deserialize_embedding,
    datetime_to_str,
    str_to_datetime
)

# Define public API
__all__ = [
    # Main facade class
    'KnowledgeGraph',
    
    # Core model classes
    'Entity', 
    'Observation', 
    'Relation',
    
    # Database utilities
    'init_database',
    'get_connection',
    
    # Utility functions
    'get_cache_key',
    'invalidate_cache',
    'execute_with_retry',
    'serialize_embedding',
    'deserialize_embedding',
    'datetime_to_str',
    'str_to_datetime'
]
