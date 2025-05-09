"""
Knowledge Graph Relation API Module.

This module provides a clean API for relation operations in the Knowledge Graph.
It's part of the facade pattern implementation to maintain backward compatibility
while refactoring the monolithic KnowledgeGraph class into multiple focused modules.
"""

import logging
from typing import Dict, List, Optional, Any

from ..features.knowledge_graph_relations.relation_manager import RelationManager
from ..knowledge_graph_core_facade.kg_models_all import Relation
from ..core.exceptions import KnowledgeGraphError, EntityNotFoundError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_relation_api")

class KnowledgeGraphRelationAPI:
    """
    API for relation operations in the Knowledge Graph.
    
    This class provides a clean interface for relation operations while delegating
    the actual implementation to the RelationManager. It's part of the facade pattern
    to maintain backward compatibility during the refactoring of the monolithic
    KnowledgeGraph class.
    """
    
    def __init__(self, relation_manager: RelationManager):
        """
        Initialize the Knowledge Graph Relation API.
        
        Args:
            relation_manager: RelationManager instance to delegate operations to
        """
        self._relation_manager = relation_manager
        logger.debug("KnowledgeGraphRelationAPI initialized")
    
    def create_relation(
        self, 
        from_entity_id: str, 
        to_entity_id: str, 
        relation_type: str,
        confidence: float = 1.0,
        properties: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a relation between two entities.
        
        Args:
            from_entity_id: ID of the source entity
            to_entity_id: ID of the target entity
            relation_type: Type of the relation
            confidence: Confidence score for the relation (0.0 to 1.0)
            properties: Optional additional properties for the relation
            
        Returns:
            ID of the created relation
            
        Raises:
            EntityNotFoundError: If either entity does not exist
            KnowledgeGraphError: If an error occurs while creating the relation
        """
        try:
            logger.debug(f"Creating relation of type '{relation_type}' from entity {from_entity_id} to {to_entity_id}")
            relation_id = self._relation_manager.create_relation(
                from_entity_id=from_entity_id,
                to_entity_id=to_entity_id,
                relation_type=relation_type,
                confidence=confidence,
                properties=properties
            )
            logger.debug(f"Relation created with ID: {relation_id}")
            return relation_id
        except EntityNotFoundError:
            # Re-raise EntityNotFoundError without wrapping it
            raise
        except Exception as e:
            error_msg = f"Error creating relation from '{from_entity_id}' to '{to_entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def get_relations(
        self, 
        entity_id: str, 
        direction: str = "both", 
        relation_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get relations for an entity.
        
        Args:
            entity_id: ID of the entity
            direction: Relation direction - 'outgoing', 'incoming', or 'both'
            relation_type: Optional filter for relation type
            
        Returns:
            List of relation dictionaries
            
        Raises:
            EntityNotFoundError: If the entity does not exist
            KnowledgeGraphError: If an error occurs while retrieving relations
        """
        try:
            logger.debug(f"Getting relations for entity {entity_id}, direction: {direction}, type: {relation_type}")
            relations = self._relation_manager.get_relations(
                entity_id=entity_id,
                direction=direction,
                relation_type=relation_type
            )
            logger.debug(f"Retrieved {len(relations)} relations for entity {entity_id}")
            return relations
        except EntityNotFoundError:
            # Re-raise EntityNotFoundError without wrapping it
            raise
        except Exception as e:
            error_msg = f"Error retrieving relations for entity '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
    def delete_relation(self, relation_id: str) -> bool:
        """
        Delete a relation.
        
        Args:
            relation_id: ID of the relation to delete
            
        Returns:
            True if the relation was deleted, False if not found
            
        Raises:
            KnowledgeGraphError: If an error occurs while deleting the relation
        """
        try:
            logger.debug(f"Deleting relation with ID: {relation_id}")
            result = self._relation_manager.delete_relation(relation_id=relation_id)
            
            if result:
                logger.debug(f"Relation with ID {relation_id} deleted successfully")
            else:
                logger.warning(f"Relation with ID {relation_id} not found for deletion")
                
            return result
        except Exception as e:
            error_msg = f"Error deleting relation with ID '{relation_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e