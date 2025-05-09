"""
Maintenance Manager for Knowledge Graph operations.

This module provides a MaintenanceManager class that handles maintenance
operations for the knowledge graph, such as backup, restore, clearing data,
and retrieving statistics.
"""

from typing import Dict, List, Optional, Any, Tuple

from .ops_maintenance import (
    clear_knowledge_graph,
    backup_knowledge_graph,
    restore_knowledge_graph,
    get_knowledge_graph_stats
)
from ...core.exceptions import KnowledgeGraphError
from ..common_kg_services.base_manager import BaseManager

class MaintenanceManager(BaseManager):
    """
    Manager for maintenance operations in the knowledge graph.
    
    Handles operations such as backup, restore, clearing data, and retrieving
    statistics about the knowledge graph.
    """
    
    def clear(self) -> Dict[str, int]:
        """
        Clear all data from the knowledge graph.
        
        Returns:
            Dictionary with counts of deleted items
            
        Raises:
            KnowledgeGraphError: If an error occurs while clearing the data
        """
        with self._lock:
            return clear_knowledge_graph(
                self.conn,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )
    
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
        with self._lock:
            return backup_knowledge_graph(
                db_path=self.db_path, # Removed self.conn
                backup_dir=backup_path, # Renamed backup_path to backup_dir
                context_logger=self.context_logger
            )
    
    def restore(self, backup_path: str) -> bool:
        """
        Restore the knowledge graph from a backup.
        
        Args:
            backup_path: Path to the backup database file
            
        Returns:
            True if the restore was successful
            
        Raises:
            KnowledgeGraphError: If an error occurs during restore
        """
        with self._lock:
            return restore_knowledge_graph(
                db_path=self.db_path,
                backup_file_path=backup_path, # Corrected keyword argument
                context_logger=self.context_logger
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the knowledge graph.
        
        Returns:
            Dictionary of statistics
            
        Raises:
            KnowledgeGraphError: If an error occurs while collecting statistics
        """
        return get_knowledge_graph_stats(
            self.conn,
            self.db_path,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl
        )