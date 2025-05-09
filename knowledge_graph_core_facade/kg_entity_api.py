"""
Knowledge Graph Entity API Module.

This module provides a clean API for entity operations in the Knowledge Graph.
It's part of the facade pattern implementation to maintain backward compatibility
while refactoring the monolithic KnowledgeGraph class into multiple focused modules.
"""

import logging
from typing import Dict, List, Optional, Any

from ..features.knowledge_graph_entities.entity_manager import EntityManager
from ..knowledge_graph_core_facade.kg_models_all import Entity
from ..core.exceptions import KnowledgeGraphError, EntityNotFoundError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_entity_api")

class KnowledgeGraphEntityAPI:
    """
    API for entity operations in the Knowledge Graph.
    
    This class provides a clean interface for entity operations while delegating
    the actual implementation to the EntityManager. It's part of the facade pattern
    to maintain backward compatibility during the refactoring of the monolithic
    KnowledgeGraph class.
    """
    
    def __init__(self, entity_manager: EntityManager):
        """
        Initialize the Knowledge Graph Entity API.
        
        Args:
            entity_manager: EntityManager instance to delegate operations to
        """
        self._entity_manager = entity_manager
        logger.debug("KnowledgeGraphEntityAPI initialized")
    
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
        try:
            logger.debug(f"Creating entity: {name} of type {entity_type}")
            entity_id = self._entity_manager.create_entity(
                name=name,
                entity_type=entity_type,
                embedding=embedding,
                properties=properties
            )
            logger.debug(f"Entity created with ID: {entity_id}")
            return entity_id
        except Exception as e:
            error_msg = f"Error creating entity '{name}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def get_entity(self, entity_id: str) -> Entity:
        """
        Get an entity by its ID.
        
        Args:
            entity_id: ID of the entity to retrieve
            
        Returns:
            Entity object
            
        Raises:
            EntityNotFoundError: If the entity is not found
            KnowledgeGraphError: If an error occurs while retrieving the entity
        """
        try:
            logger.debug(f"Getting entity with ID: {entity_id}")
            entity = self._entity_manager.get_entity(entity_id=entity_id)
            
            if entity is None:
                error_msg = f"Entity with ID '{entity_id}' not found"
                logger.warning(error_msg)
                raise EntityNotFoundError(error_msg)
                
            return entity
        except EntityNotFoundError:
            raise
        except Exception as e:
            error_msg = f"Error retrieving entity with ID '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def get_entity_by_name(self, name: str) -> Entity:
        """
        Get an entity by its name.
        
        Args:
            name: Name of the entity to retrieve
            
        Returns:
            Entity object
            
        Raises:
            EntityNotFoundError: If the entity is not found
            KnowledgeGraphError: If an error occurs while retrieving the entity
        """
        try:
            logger.debug(f"Getting entity by name: {name}")
            entity = self._entity_manager.get_entity_by_name(name=name)
            
            if entity is None:
                error_msg = f"Entity with name '{name}' not found"
                logger.warning(error_msg)
                raise EntityNotFoundError(error_msg)
                
            return entity
        except EntityNotFoundError:
            raise
        except Exception as e:
            error_msg = f"Error retrieving entity with name '{name}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
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
        try:
            logger.debug(f"Updating entity with ID: {entity_id}")
            result = self._entity_manager.update_entity(
                entity_id=entity_id,
                name=name,
                entity_type=entity_type,
                embedding=embedding,
                properties=properties
            )
            
            if result:
                logger.debug(f"Entity with ID {entity_id} updated successfully")
            else:
                logger.warning(f"Entity with ID {entity_id} not found for update")
                
            return result
        except Exception as e:
            error_msg = f"Error updating entity with ID '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
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
        try:
            logger.debug(f"Deleting entity with ID: {entity_id}")
            result = self._entity_manager.delete_entity(entity_id=entity_id)
            
            if result:
                logger.debug(f"Entity with ID {entity_id} deleted successfully")
            else:
                logger.warning(f"Entity with ID {entity_id} not found for deletion")
                
            return result
        except Exception as e:
            error_msg = f"Error deleting entity with ID '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e