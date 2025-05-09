"""
Utility functions for the Knowledge Graph component.

This module provides common utility functions used across the Knowledge Graph
operations, including caching, serialization, and database helpers.
"""

import json
import hashlib
import logging
import sqlite3
# import importlib.util # F401 unused
from typing import Dict, Any, Optional, List, TypeVar, Union # Added Union back
from datetime import datetime

logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_utils")

# Type aliases for improved readability
CacheKeyType = str
JsonSerializable = Union[Dict, List, str, int, float, bool, None]
T = TypeVar('T')

# Cache utilities with dependency injection


def get_cache_key(operation: str, *args, **kwargs) -> CacheKeyType: # E302
    """
    Generate a cache key for any cache provider.
    
    Args:
        operation: Name of the operation (e.g., 'get_entity', 'search_entities')
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        A cache key string
    """
    # Combine operation and arguments into a string
    key_parts = [operation]
    for arg in args:
        key_parts.append(str(arg))
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}={v}")
    
    # Create a hash of the combined string
    key_string = ":".join(key_parts)
    hashed = hashlib.md5(key_string.encode()).hexdigest()
    return f"kg:{operation}:{hashed}"


def invalidate_cache(cache_provider: Optional[Any], pattern: str = "kg:*") -> None: # E302
    """
    Invalidate cache entries that match a pattern.
    
    Args:
        cache_provider: Cache provider instance (must implement delete and keys methods)
        pattern: Cache key pattern to match
    """
    if not cache_provider:
        return
        
    try:
        # Check if the cache provider has a keys method
        if hasattr(cache_provider, 'keys'):
            # Find all keys matching the pattern
            keys = cache_provider.keys(pattern)
            if keys:
                # Delete all matching keys
                cache_provider.delete(*keys)
                logger.debug(f"Invalidated {len(keys)} cache entries with pattern {pattern}")
        else:
            # Fallback to just deleting the pattern directly
            # Some cache providers might support pattern deletion directly
            cache_provider.delete(pattern)
            logger.debug(f"Invalidated cache entries with pattern {pattern}")
    except Exception as e:
        logger.warning("Error invalidating cache. See exception details.", exc_info=e)


def execute_with_retry(cursor: sqlite3.Cursor, query: str, params=None, max_retries: int = 3): # E302
    """
    Execute a SQL query with retry logic for handling busy database issues.
    
    Args:
        cursor: SQLite cursor
        query: SQL query string
        params: Query parameters
        max_retries: Maximum number of retry attempts
        
    Returns:
        Cursor after executing the query
    """
    params = params or []
    retry_count = 0
    while retry_count < max_retries:
        try:
            return cursor.execute(query, params)
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and retry_count < max_retries - 1:
                retry_count += 1
                logger.warning(f"Database locked, retrying ({retry_count}/{max_retries})...")
                import time
                time.sleep(0.1 * (2 ** retry_count))  # Exponential backoff
            else:
                raise
        except Exception:
            raise


def serialize_embedding(embedding: Optional[List[float]]) -> Optional[str]: # E302
    """
    Serialize an embedding vector to a JSON string.
    
    Args:
        embedding: Embedding vector or None
        
    Returns:
        JSON string or None
    """
    if embedding is None:
        return None
    return json.dumps(embedding)


def deserialize_embedding(embedding_json: Optional[str]) -> Optional[List[float]]: # E302
    """
    Deserialize an embedding vector from a JSON string.
    
    Args:
        embedding_json: JSON string or None
        
    Returns:
        Embedding vector or None
    """
    if embedding_json is None or embedding_json == "":
        return None
    return json.loads(embedding_json)


def datetime_to_str(dt: datetime) -> str: # E302
    """
    Convert a datetime object to ISO format string.
    
    Args:
        dt: Datetime object
        
    Returns:
        ISO format datetime string
    """
    return dt.isoformat()


def str_to_datetime(dt_str: str) -> datetime: # E302
    """
    Convert an ISO format string to a datetime object.
    
    Args:
        dt_str: ISO format datetime string
        
    Returns:
        Datetime object
    """
    return datetime.fromisoformat(dt_str)

