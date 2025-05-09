"""
Search Manager for Knowledge Graph operations.

This module provides a SearchManager class that handles operations related
to searching for entities in the knowledge graph.
"""

from typing import Dict, List, Optional, Any

from .search_ops import search_entities
from ...core.exceptions import KnowledgeGraphError
from ..common_kg_services.base_manager import BaseManager

class SearchManager(BaseManager):
    """
    Manager for search operations in the knowledge graph.
    
    Handles semantic and textual search operations across entities.
    """
    
    def search_entities(
        self, 
        query: str, 
        entity_type: Optional[str] = None,
        limit: int = 10,
        min_similarity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Search for entities by name, type, or content.
        
        Args:
            query: Search query
            entity_type: Optional entity type filter
            limit: Maximum number of results to return
            min_similarity: Minimum similarity score (0.0 to 1.0)
            
        Returns:
            List of matching entities with similarity scores
            
        Raises:
            KnowledgeGraphError: If an error occurs during search
        """
        return search_entities(
            self.conn,
            query=query,
            entity_type=entity_type,
            limit=limit,
            min_similarity=min_similarity,
            embedding_function=self.embedding_function,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl,
            context_logger=self.context_logger
        )