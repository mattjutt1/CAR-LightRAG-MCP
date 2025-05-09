"""
Base Manager for Knowledge Graph operations.

This module provides a BaseManager class that contains common functionality
for all Knowledge Graph manager classes.
"""

import threading
import logging
from typing import Any, Optional

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph.managers")

class BaseManager:
    """
    Base class for all knowledge graph manager classes.
    
    This class provides common functionality such as connection handling,
    caching, context logging, and thread safety.
    """
    
    def __init__(
        self,
        conn,
        db_path: str,
        redis_client = None,
        context_logger = None,
        embedding_function = None,
        cache_ttl: int = 3600,  # 1 hour default TTL
        lock = None
    ):
        """
        Initialize the base manager with common dependencies.
        
        Args:
            conn: SQLite database connection
            db_path: Path to the SQLite database file
            redis_client: Optional Redis client for caching
            context_logger: Optional logger for context events
            embedding_function: Optional function to generate embeddings
            cache_ttl: Time-to-live for cached results in seconds
            lock: Thread lock for synchronization (shared across managers)
        """
        self.conn = conn
        self.db_path = db_path
        self.redis_client = redis_client
        self.context_logger = context_logger
        self.embedding_function = embedding_function
        self.cache_ttl = cache_ttl
        
        # Use provided lock or create a new one
        self._lock = lock if lock is not None else threading.RLock()
    
    def log_event(self, event_name: str, data: Any = None) -> None:
        """
        Log an event using the context logger if available.
        
        Args:
            event_name: Name of the event
            data: Optional data to log with the event
        """
        if self.context_logger:
            self.context_logger.log_event(event_name, data)