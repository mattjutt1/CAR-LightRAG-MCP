"""
Entity Manager for Knowledge Graph operations.

This module provides an EntityManager class that handles operations related
to entities in the knowledge graph, such as creation, retrieval, updating,
and deletion.
"""

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
from ..common_kg_services.base_manager import BaseManager

class EntityManager(BaseManager):
    """
    Manager for entity operations in the knowledge graph.
    
    Handles creation, retrieval, updating, and deletion of entities.
    """
    
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
        with self._lock:
            return create_entity(
                self.conn,
                name=name,
                entity_type=entity_type,
                embedding_function=self.embedding_function,
                embedding=embedding,
                properties=properties,
                redis_client=self.redis_client,
                context_logger=self.context_logger,
                cache_ttl=self.cache_ttl # Pass cache_ttl
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
            self.conn,
            entity_id=entity_id,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl,
            context_logger=self.context_logger
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
            self.conn,
            name=name,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl,
            context_logger=self.context_logger
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
        with self._lock:
            return update_entity(
                self.conn,
                entity_id=entity_id,
                name=name,
                entity_type=entity_type,
                embedding=embedding,
                properties=properties,
                redis_client=self.redis_client,
                context_logger=self.context_logger
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
        with self._lock:
            return delete_entity(
                self.conn,
                entity_id=entity_id,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )