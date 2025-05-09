"""
Main Knowledge Graph implementation for the CAR/LightRAG MCP server.

This module implements the KnowledgeGraph class which serves as a facade
for the various Knowledge Graph operations, delegating to specialized
modules for entity, observation, relation, search, and maintenance
operations.
"""

import os
import logging
from typing import Dict, List, Optional, Any, Tuple

# Import connection manager
from .kg_connection import KnowledgeGraphConnection

# Import factory methods
from .kg_factory import (
    create_entity_manager,
    create_observation_manager,
    create_relation_manager,
    create_search_manager,
    create_maintenance_manager
)

# Import API classes
from .kg_entity_api import KnowledgeGraphEntityAPI
from .kg_observation_api import KnowledgeGraphObservationAPI
from .kg_relation_api import KnowledgeGraphRelationAPI
from .kg_search_api import KnowledgeGraphSearchAPI
from .kg_maintenance_api import KnowledgeGraphMaintenanceAPI

# Import model classes
from .kg_models_all import Entity, Observation

# Import exceptions
from ..core.exceptions import KnowledgeGraphError, EntityNotFoundError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.graph_facade")

class KnowledgeGraph:
    """
    Knowledge Graph for code understanding and relationship management.
    
    Implements an Entity-Relation-Observation model using SQLite as the
    backend. Supports storing embeddings for semantic search and provides
    efficient querying capabilities.
    
    This class serves as a facade that delegates operations to specialized API
    classes, which in turn use manager classes for the actual implementation.
    """
    
    def __init__(
        self,
        db_path: str,
        redis_client=None,
        context_logger=None,
        embedding_function=None,
        cache_ttl: int = 3600  # 1 hour default TTL
    ):
        """
        Initialize the Knowledge Graph with a SQLite database.
        
        Args:
            db_path: Path to the SQLite database file
            redis_client: Optional Redis client for caching
            context_logger: Optional logger for context events
            embedding_function: Optional function to generate embeddings
            cache_ttl: Time-to-live for cached results in seconds
        """
        self.db_path = db_path
        self.redis_client = redis_client
        self.context_logger = context_logger
        self.embedding_function = embedding_function
        self.cache_ttl = cache_ttl
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        try:
            # Initialize the connection manager
            self._connection = KnowledgeGraphConnection(
                db_path=db_path,
                redis_client=redis_client,
                context_logger=context_logger
            )
            
            # Initialize managers using factory methods
            self._entity_manager = create_entity_manager(
                connection=self._connection,
                embedding_function=embedding_function,
                cache_ttl=cache_ttl
            )
            
            self._observation_manager = create_observation_manager(
                connection=self._connection,
                embedding_function=embedding_function,
                cache_ttl=cache_ttl
            )
            
            self._relation_manager = create_relation_manager(
                connection=self._connection,
                cache_ttl=cache_ttl
            )
            
            self._search_manager = create_search_manager(
                connection=self._connection,
                embedding_function=embedding_function,
                cache_ttl=cache_ttl
            )
            
            self._maintenance_manager = create_maintenance_manager(
                connection=self._connection,
                cache_ttl=cache_ttl
            )
            
            # Initialize API instances
            self._entity_api = KnowledgeGraphEntityAPI(
                entity_manager=self._entity_manager
            )
            
            self._observation_api = KnowledgeGraphObservationAPI(
                observation_manager=self._observation_manager
            )
            
            self._relation_api = KnowledgeGraphRelationAPI(
                relation_manager=self._relation_manager
            )
            
            self._search_api = KnowledgeGraphSearchAPI(
                search_manager=self._search_manager
            )
            
            self._maintenance_api = KnowledgeGraphMaintenanceAPI(
                maintenance_manager=self._maintenance_manager,
                connection=self._connection
            )
            
            logger.info(f"Knowledge Graph initialized with database at {db_path}")
            
            if self.context_logger:
                stats = self._maintenance_api.get_stats()
                self.context_logger.log_event(
                    "Knowledge Graph Initialized",
                    {"db_path": db_path, "stats": stats}
                )
                
        except Exception as e:
            error_msg = f"Failed to initialize Knowledge Graph database: {str(e)}"
            logger.error(error_msg)
            if self.context_logger:
                self.context_logger.log_event(
                    "Knowledge Graph Initialization Error",
                    {"error": error_msg}
                )
            raise KnowledgeGraphError(error_msg) from e
    
    # Entity operations
    
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
        return self._entity_api.create_entity(
            name=name,
            entity_type=entity_type,
            embedding=embedding,
            properties=properties
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
        try:
            return self._entity_api.get_entity(entity_id=entity_id)
        except EntityNotFoundError:
            return None
    
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
        try:
            return self._entity_api.get_entity_by_name(name=name)
        except EntityNotFoundError:
            return None
    
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
        return self._entity_api.update_entity(
            entity_id=entity_id,
            name=name,
            entity_type=entity_type,
            embedding=embedding,
            properties=properties
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
        return self._entity_api.delete_entity(entity_id=entity_id)
    
    # Observation operations
    
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
        return self._observation_api.add_observation(
            entity_id=entity_id,
            observation=observation,
            embedding=embedding,
            properties=properties
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
        return self._observation_api.get_observations(
            entity_id=entity_id,
            limit=limit
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
        return self._observation_api.delete_observation(
            observation_id=observation_id
        )
    
    # Relation operations
    
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
        return self._relation_api.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type=relation_type,
            confidence=confidence,
            properties=properties
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
        return self._relation_api.get_relations(
            entity_id=entity_id,
            direction=direction,
            relation_type=relation_type
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
        return self._relation_api.delete_relation(relation_id=relation_id)
    
    # Search operations
    
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
        return self._search_api.search_entities(
            query=query,
            entity_type=entity_type,
            limit=limit,
            min_similarity=min_similarity
        )
    
    # Maintenance operations
    
    def clear(self) -> Dict[str, int]:
        """
        Clear all data from the knowledge graph.
        
        Returns:
            Dictionary with counts of deleted items
            
        Raises:
            KnowledgeGraphError: If an error occurs while clearing the data
        """
        return self._maintenance_api.clear()
    
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
        return self._maintenance_api.backup(backup_path=backup_path)
    
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
        success = self._maintenance_api.restore(backup_path=backup_path)
        
        if success:
            # Re-initialize the connection, managers, and APIs
            try:
                # Store current values needed for re-initialization
                current_db_path = self.db_path
                current_redis_client = self.redis_client
                current_context_logger = self.context_logger
                current_embedding_function = self.embedding_function
                current_cache_ttl = self.cache_ttl
                
                # Re-initialize the connection manager
                self._connection = KnowledgeGraphConnection(
                    db_path=current_db_path,
                    redis_client=current_redis_client,
                    context_logger=current_context_logger
                )
                
                # Re-initialize managers using factory methods
                self._entity_manager = create_entity_manager(
                    connection=self._connection,
                    embedding_function=current_embedding_function,
                    cache_ttl=current_cache_ttl
                )
                
                self._observation_manager = create_observation_manager(
                    connection=self._connection,
                    embedding_function=current_embedding_function,
                    cache_ttl=current_cache_ttl
                )
                
                self._relation_manager = create_relation_manager(
                    connection=self._connection,
                    cache_ttl=current_cache_ttl
                )
                
                self._search_manager = create_search_manager(
                    connection=self._connection,
                    embedding_function=current_embedding_function,
                    cache_ttl=current_cache_ttl
                )
                
                self._maintenance_manager = create_maintenance_manager(
                    connection=self._connection,
                    cache_ttl=current_cache_ttl
                )
                
                # Re-initialize API instances
                self._entity_api = KnowledgeGraphEntityAPI(
                    entity_manager=self._entity_manager
                )
                
                self._observation_api = KnowledgeGraphObservationAPI(
                    observation_manager=self._observation_manager
                )
                
                self._relation_api = KnowledgeGraphRelationAPI(
                    relation_manager=self._relation_manager
                )
                
                self._search_api = KnowledgeGraphSearchAPI(
                    search_manager=self._search_manager
                )
                
                self._maintenance_api = KnowledgeGraphMaintenanceAPI(
                    maintenance_manager=self._maintenance_manager,
                    connection=self._connection
                )
                
                # Invalidate stats cache after successful restore and re-initialization
                if self.redis_client:
                    # Local import to avoid circularity if kg_utils imports from graph_facade
                    from .kg_utils import invalidate_cache
                    invalidate_cache(self.redis_client, "kg:stats")
                    logger.info("Invalidated 'kg:stats' cache after restore.")
                
                logger.info(
                    "Knowledge Graph connection, managers, and APIs re-initialized "
                    f"after restore from {backup_path}"
                )
                
            except Exception as e_reinit:
                error_msg = f"Failed to re-initialize Knowledge Graph after restore: {str(e_reinit)}"
                logger.error(error_msg)
                # This is a critical state, the KG might be unusable.
                raise KnowledgeGraphError(error_msg) from e_reinit
                
        return success
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the knowledge graph.
        
        Returns:
            Dictionary of statistics
            
        Raises:
            KnowledgeGraphError: If an error occurs while collecting statistics
        """
        return self._maintenance_api.get_stats()
    
    def close(self) -> None:
        """Close the database connection."""
        if hasattr(self, '_connection'):
            self._connection.close()
            logger.info("Knowledge graph database connection closed")