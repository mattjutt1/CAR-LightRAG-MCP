"""
Knowledge Graph Maintenance API Module.

This module provides a clean API for maintenance operations in the Knowledge Graph.
It's part of the facade pattern implementation to maintain backward compatibility
while refactoring the monolithic KnowledgeGraph class into multiple focused modules.
"""

import logging
from typing import Dict, Tuple, Any

from ..features.knowledge_graph_maintenance.maintenance_manager import MaintenanceManager
from .kg_connection import KnowledgeGraphConnection
from ..core.exceptions import KnowledgeGraphError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_maintenance_api")

class KnowledgeGraphMaintenanceAPI:
    """
    API for maintenance operations in the Knowledge Graph.
    
    This class provides a clean interface for maintenance operations while delegating
    the actual implementation to the MaintenanceManager. It's part of the facade pattern
    to maintain backward compatibility during the refactoring of the monolithic
    KnowledgeGraph class.
    
    Maintenance operations include:
    - Clearing the knowledge graph
    - Backing up the knowledge graph database
    - Restoring the knowledge graph from a backup
    - Getting statistics about the knowledge graph
    """
    
    def __init__(self, maintenance_manager: MaintenanceManager, connection: KnowledgeGraphConnection):
        """
        Initialize the Knowledge Graph Maintenance API.
        
        Args:
            maintenance_manager: MaintenanceManager instance to delegate operations to
            connection: KnowledgeGraphConnection instance for managing the database connection
        """
        self._maintenance_manager = maintenance_manager
        self._connection = connection
        logger.debug("KnowledgeGraphMaintenanceAPI initialized")
    
    def clear(self) -> Dict[str, int]:
        """
        Clear all data from the knowledge graph.
        
        Returns:
            Dictionary with counts of deleted items
            
        Raises:
            KnowledgeGraphError: If an error occurs while clearing the data
        """
        try:
            logger.debug("Clearing knowledge graph data")
            result = self._maintenance_manager.clear()
            logger.debug(f"Knowledge graph cleared: {result}")
            return result
        except Exception as e:
            error_msg = f"Error clearing knowledge graph: {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def backup(self, backup_path: str) -> Tuple[str, str]:
        """
        Create a backup of the knowledge graph database.
        
        Args:
            backup_path: Directory to store the backup
            
        Returns:
            Tuple of (database backup path, stats path)
            
        Raises:
            KnowledgeGraphError: If an error occurs during backup
        """
        try:
            logger.debug(f"Creating knowledge graph backup in directory: {backup_path}")
            result = self._maintenance_manager.backup(backup_path)
            logger.debug(f"Knowledge graph backup created: {result}")
            return result
        except Exception as e:
            error_msg = f"Error creating knowledge graph backup: {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def restore(self, backup_path: str) -> bool:
        """
        Restore the knowledge graph from a backup.
        
        This method also handles closing and reinitializing the database connection
        after the restore operation.
        
        Args:
            backup_path: Path to the backup database file
            
        Returns:
            True if the restore was successful
            
        Raises:
            KnowledgeGraphError: If an error occurs during restore
        """
        try:
            logger.debug(f"Restoring knowledge graph from backup: {backup_path}")
            
            # First, close the current connection
            self._connection.close()
            
            # Perform the restore operation
            result = self._maintenance_manager.restore(backup_path)
            
            # Reinitialize the connection after restore
            # The connection will be lazily initialized on next use
            
            logger.debug(f"Knowledge graph restored from backup: {result}")
            return result
        except Exception as e:
            error_msg = f"Error restoring knowledge graph: {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the knowledge graph.
        
        Returns:
            Dictionary of statistics
            
        Raises:
            KnowledgeGraphError: If an error occurs while collecting statistics
        """
        try:
            logger.debug("Getting knowledge graph statistics")
            result = self._maintenance_manager.get_stats()
            logger.debug("Knowledge graph statistics retrieved")
            return result
        except Exception as e:
            error_msg = f"Error getting knowledge graph statistics: {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e