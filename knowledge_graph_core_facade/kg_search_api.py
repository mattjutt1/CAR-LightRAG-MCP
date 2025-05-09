"""
Knowledge Graph Search API Module.

This module provides a clean API for search operations in the Knowledge Graph.
It's part of the facade pattern implementation to maintain backward compatibility
while refactoring the monolithic KnowledgeGraph class into multiple focused modules.
"""

import logging
from typing import Dict, List, Optional, Any

from ..features.knowledge_graph_search.search_manager import SearchManager
from ..core.exceptions import KnowledgeGraphError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_search_api")

class KnowledgeGraphSearchAPI:
    """
    API for search operations in the Knowledge Graph.
    
    This class provides a clean interface for search operations while delegating
    the actual implementation to the SearchManager. It's part of the facade pattern
    to maintain backward compatibility during the refactoring of the monolithic
    KnowledgeGraph class.
    """
    
    def __init__(self, search_manager: SearchManager):
        """
        Initialize the Knowledge Graph Search API.
        
        Args:
            search_manager: SearchManager instance to delegate operations to
        """
        self._search_manager = search_manager
        logger.debug("KnowledgeGraphSearchAPI initialized")
    
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
        try:
            logger.debug(f"Searching entities with query: '{query}', type: {entity_type}, limit: {limit}")
            results = self._search_manager.search_entities(
                query=query,
                entity_type=entity_type,
                limit=limit,
                min_similarity=min_similarity
            )
            logger.debug(f"Found {len(results)} entities matching query '{query}'")
            return results
        except Exception as e:
            error_msg = f"Error searching entities with query '{query}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e