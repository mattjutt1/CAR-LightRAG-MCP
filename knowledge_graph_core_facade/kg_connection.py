"""
Knowledge Graph Connection Management Module.

This module provides thread-safe access to the SQLite database connection
used by the Knowledge Graph. It's responsible for initializing, managing,
and closing database connections.
"""

import os
import logging
import threading
import sqlite3
from pathlib import Path
from typing import Optional

from ..core.exceptions import KnowledgeGraphError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_connection")

class KnowledgeGraphConnection:
    """
    Manages the database connection for the Knowledge Graph.
    
    This class is responsible for:
    - Initializing the database connection
    - Providing thread-safe access to the connection
    - Managing connection lifecycle
    - Handling connection errors
    
    Thread safety is ensured through the use of a reentrant lock (RLock).
    """
    
    def __init__(
        self,
        db_path: str,
        redis_client=None,
        context_logger=None
    ):
        """
        Initialize the Knowledge Graph connection manager.
        
        Args:
            db_path: Path to the SQLite database file
            redis_client: Optional Redis client for caching
            context_logger: Optional logger for context events
            
        Raises:
            KnowledgeGraphError: If the database connection cannot be initialized
        """
        self.db_path = db_path
        self.redis_client = redis_client
        self.context_logger = context_logger
        
        # Create a lock for thread safety
        self._lock = threading.RLock()
        
        # Connection will be initialized lazily
        self._conn: Optional[sqlite3.Connection] = None
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        try:
            # Initialize the connection
            self._initialize_connection()
            
            if self.context_logger:
                self.context_logger.log_event(
                    "Knowledge Graph Connection Initialized",
                    {"db_path": db_path}
                )
                
            logger.info(f"Knowledge Graph connection initialized with database at {db_path}")
            
        except Exception as e:
            error_msg = f"Failed to initialize Knowledge Graph database connection: {str(e)}"
            logger.error(error_msg)
            if self.context_logger:
                self.context_logger.log_event(
                    "Knowledge Graph Connection Initialization Error",
                    {"error": error_msg}
                )
            raise KnowledgeGraphError(error_msg) from e
    
    def _initialize_connection(self) -> None:
        """
        Initialize the SQLite database connection.
        
        This method sets up the connection with proper row factory and
        enables foreign keys support.
        
        Raises:
            KnowledgeGraphError: If the database connection cannot be initialized
        """
        try:
            # Ensure the directory exists
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Connect to the database
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            
            # Enable foreign keys
            self._conn.execute("PRAGMA foreign_keys = ON")
            
        except sqlite3.Error as e:
            error_msg = f"SQLite error during connection initialization: {str(e)}"
            logger.error(error_msg)
            if self._conn:
                self._conn.close()
                self._conn = None
            raise KnowledgeGraphError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during connection initialization: {str(e)}"
            logger.error(error_msg)
            if self._conn:
                self._conn.close()
                self._conn = None
            raise KnowledgeGraphError(error_msg) from e
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get the SQLite database connection.
        
        Returns:
            The SQLite connection object
            
        Raises:
            KnowledgeGraphError: If the connection is not initialized or has been closed
        """
        with self._lock:
            if not self._conn:
                try:
                    self._initialize_connection()
                except Exception as e:
                    raise KnowledgeGraphError(f"Failed to re-initialize database connection: {str(e)}") from e
            
            return self._conn
    
    def get_lock(self) -> threading.RLock:
        """
        Get the thread lock used for synchronizing database operations.
        
        Returns:
            The reentrant lock object
        """
        return self._lock
    
    def close(self) -> None:
        """
        Close the database connection.
        
        This method should be called when the connection is no longer needed
        to release resources.
        """
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                    logger.info("Knowledge graph database connection closed")
                    
                    if self.context_logger:
                        self.context_logger.log_event(
                            "Knowledge Graph Connection Closed",
                            {"db_path": self.db_path}
                        )
                except Exception as e:
                    logger.error(f"Error closing database connection: {str(e)}")
                finally:
                    self._conn = None