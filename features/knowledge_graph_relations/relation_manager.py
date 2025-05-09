"""
Relation Manager for Knowledge Graph operations.

This module provides a RelationManager class that handles operations related
to relations between entities in the knowledge graph, such as creating,
retrieving, and deleting relations.
"""

from typing import Dict, List, Optional, Any

from .ops_relation_crud import (
    create_relation,
    get_relations,
    delete_relation
)
from ...core.exceptions import KnowledgeGraphError, EntityNotFoundError
from ..common_kg_services.base_manager import BaseManager

class RelationManager(BaseManager):
    """
    Manager for relation operations in the knowledge graph.
    
    Handles creating, retrieving, and deleting relations between entities.
    """
    
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
        with self._lock:
            return create_relation(
                self.conn,
                from_entity_id=from_entity_id,
                to_entity_id=to_entity_id,
                relation_type=relation_type,
                confidence=confidence,
                properties=properties,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )
    
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
        return get_relations(
            self.conn,
            entity_id=entity_id,
            direction=direction,
            relation_type=relation_type,
            redis_client=self.redis_client,
            cache_ttl=self.cache_ttl,
            context_logger=self.context_logger
        )
    
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
        with self._lock:
            return delete_relation(
                self.conn,
                relation_id=relation_id,
                redis_client=self.redis_client,
                context_logger=self.context_logger
            )