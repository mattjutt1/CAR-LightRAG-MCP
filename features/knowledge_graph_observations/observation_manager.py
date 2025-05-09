"""
Observation Manager for Knowledge Graph operations.

This module provides an ObservationManager class that handles operations related
to observations in the knowledge graph, such as adding, retrieving, and deleting
observations.
"""

from typing import Dict, List, Optional, Any

from ...knowledge_graph_core_facade.kg_models_all import Observation
from .ops_observation_crud import (
    add_observation,
    get_observations,
    delete_observation
)
from ...core.exceptions import KnowledgeGraphError, EntityNotFoundError
from ..common_kg_services.base_manager import BaseManager

class ObservationManager(BaseManager):
    """
    Manager for observation operations in the knowledge graph.
    
    Handles adding, retrieving, and deleting observations for entities.
    """
    
    def add_observation(
        self, 
        entity_id: str, 
        observation: str,
        embedding: Optional[List[float]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Add an observation to an entity.
        
        Args:
            entity_id: ID of the entity
            observation: Text of the observation
            embedding: Optional embedding vector for the observation
            properties: Optional additional properties for the observation
            
        Returns:
            ID of the created observation
            
        Raises:
            EntityNotFoundError: If the entity does not exist
            KnowledgeGraphError: If an error occurs while adding the observation
        """
        with self._lock:
            return add_observation(
                self.conn,
                entity_id=entity_id,
                observation=observation,
                embedding_function=self.embedding_function,
                embedding=embedding,
                properties=properties,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )
    
    def get_observations(self, entity_id: str, limit: int = 100) -> List[Observation]:
        """
        Get observations for an entity.
        
        Args:
            entity_id: ID of the entity
            limit: Maximum number of observations to return
            
        Returns:
            List of Observation objects
            
        Raises:
            EntityNotFoundError: If the entity does not exist
            KnowledgeGraphError: If an error occurs while retrieving observations
        """
        return get_observations(
            self.conn,
            entity_id=entity_id,
            limit=limit,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl,
            context_logger=self.context_logger
        )
    
    def delete_observation(self, observation_id: str) -> bool:
        """
        Delete an observation.
        
        Args:
            observation_id: ID of the observation to delete
            
        Returns:
            True if the observation was deleted, False if not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while deleting the observation
        """
        with self._lock:
            return delete_observation(
                self.conn,
                observation_id=observation_id,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )