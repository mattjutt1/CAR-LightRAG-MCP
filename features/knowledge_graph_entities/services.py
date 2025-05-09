"""
Service layer for Knowledge Graph Entity operations.

This module defines the `EntityService` class, which provides a high-level API
for managing entities within the knowledge graph. It orchestrates calls to
lower-level operation functions (e.g., from ops_entity_crud.py) and integrates
with other components like caching and context logging.
"""
import logging
from typing import Dict, List, Optional, Any

from ...knowledge_graph_core_facade.kg_models_all import Entity
from .ops_entity_crud import (
    create_entity,
    get_entity,
    get_entity_by_name,
    update_entity,
    delete_entity
)
from ...core.exceptions import KnowledgeGraphError, EntityNotFoundError
from ..common_kg_services.base_manager import BaseManager # EntityService will inherit from BaseManager

logger = logging.getLogger("car_mcp.features.knowledge_graph_entities.services")

class EntityService(BaseManager):
    """
    Service class for managing Knowledge Graph entities.
    
    Handles creation, retrieval, updating, and deletion of entities.
    It inherits from BaseManager to get common functionalities like
    database connection, Redis client, context logger, and embedding function.
    """
    
    # __init__ is inherited from BaseManager, which should handle
    # db_connection_provider, redis_client, context_logger, embedding_function, cache_ttl, and _lock

    def create_entity(
        self, 
        name: str, 
        entity_type: str,
        embedding: Optional[List[float]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new entity in the knowledge graph.
        
        Args:
            name: Name of the entity
            entity_type: Type of the entity (e.g., 'class', 'function', 'file')
            embedding: Optional embedding vector for semantic search
            properties: Optional additional properties for the entity
            
        Returns:
            ID of the created entity
            
        Raises:
            KnowledgeGraphError: If an error occurs while creating the entity
        """
        with self._lock: # Ensure thread-safety for write operations
            return create_entity(
                self.conn, # From BaseManager
                name=name,
                entity_type=entity_type,
                embedding_function=self.embedding_function, # From BaseManager
                embedding=embedding,
                properties=properties,
                redis_client=self.redis_client, # From BaseManager
                context_logger=self.context_logger, # From BaseManager
                cache_ttl=self.cache_ttl # From BaseManager
            )
    
    def get_entity(self, entity_id: str) -> Optional[Entity]:
        """
        Get an entity by its ID.
        
        Args:
            entity_id: ID of the entity to retrieve
            
        Returns:
            Entity object or None if not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while retrieving the entity
        """
        return get_entity(
            self.conn, # From BaseManager
            entity_id=entity_id,
            redis_client=self.redis_client, # From BaseManager
            cache_ttl=self.cache_ttl, # From BaseManager
            context_logger=self.context_logger # From BaseManager
        )
    
    def get_entity_by_name(self, name: str) -> Optional[Entity]:
        """
        Get an entity by its name.
        
        Args:
            name: Name of the entity to retrieve
            
        Returns:
            Entity object or None if not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while retrieving the entity
        """
        return get_entity_by_name(
            self.conn, # From BaseManager
            name=name,
            redis_client=self.redis_client, # From BaseManager
            cache_ttl=self.cache_ttl, # From BaseManager
            context_logger=self.context_logger # From BaseManager
        )
    
    def update_entity(
        self, 
        entity_id: str, 
        name: Optional[str] = None,
        entity_type: Optional[str] = None,
        embedding: Optional[List[float]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update an existing entity.
        
        Args:
            entity_id: ID of the entity to update
            name: New name for the entity (optional)
            entity_type: New type for the entity (optional)
            embedding: New embedding vector (optional)
            properties: New properties for the entity (optional)
            
        Returns:
            True if the entity was updated, False if the entity was not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while updating the entity
        """
        with self._lock: # Ensure thread-safety for write operations
            return update_entity(
                self.conn, # From BaseManager
                entity_id=entity_id,
                name=name,
                entity_type=entity_type,
                embedding=embedding,
                properties=properties,
                redis_client=self.redis_client, # From BaseManager
                context_logger=self.context_logger # From BaseManager
            )
    
    def delete_entity(self, entity_id: str) -> bool:
        """
        Delete an entity and all its relations and observations.
        
        Args:
            entity_id: ID of the entity to delete
            
        Returns:
            True if the entity was deleted, False if the entity was not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while deleting the entity
        """
        with self._lock: # Ensure thread-safety for write operations
            return delete_entity(
                self.conn, # From BaseManager
                entity_id=entity_id,
                redis_client=self.redis_client, # From BaseManager
                context_logger=self.context_logger # From BaseManager
            )