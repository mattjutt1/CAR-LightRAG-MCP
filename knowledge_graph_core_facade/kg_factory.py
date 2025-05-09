"""
Knowledge Graph Manager Factory Module.

This module provides factory functions to create and initialize various manager
instances for the Knowledge Graph. It's part of the facade pattern implementation
to maintain backward compatibility while refactoring the monolithic KnowledgeGraph
class into multiple focused modules.
"""

import logging
from typing import Optional, Callable, List, Any

from ..features.knowledge_graph_entities.entity_manager import EntityManager
from ..features.knowledge_graph_observations.observation_manager import ObservationManager
from ..features.knowledge_graph_relations.relation_manager import RelationManager
from ..features.knowledge_graph_search.search_manager import SearchManager
from ..features.knowledge_graph_maintenance.maintenance_manager import MaintenanceManager

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_factory")

def create_entity_manager(
    connection,
    embedding_function: Optional[Callable[[str], List[float]]] = None,
    cache_ttl: int = 3600
) -> EntityManager:
    """
    Create and initialize an EntityManager instance.
    
    Args:
        connection: KnowledgeGraphConnection instance
        embedding_function: Optional function to generate embeddings from text
        cache_ttl: Time-to-live for cached results in seconds
        
    Returns:
        Initialized EntityManager instance
    """
    logger.debug("Creating EntityManager instance")
    
    return EntityManager(
        conn=connection.get_connection(),
        db_path=connection.db_path,
        redis_client=connection.redis_client,
        context_logger=connection.context_logger,
        embedding_function=embedding_function,
        cache_ttl=cache_ttl,
        lock=connection.get_lock()
    )

def create_observation_manager(
    connection,
    embedding_function: Optional[Callable[[str], List[float]]] = None,
    cache_ttl: int = 3600
) -> ObservationManager:
    """
    Create and initialize an ObservationManager instance.
    
    Args:
        connection: KnowledgeGraphConnection instance
        embedding_function: Optional function to generate embeddings from text
        cache_ttl: Time-to-live for cached results in seconds
        
    Returns:
        Initialized ObservationManager instance
    """
    logger.debug("Creating ObservationManager instance")
    
    return ObservationManager(
        conn=connection.get_connection(),
        db_path=connection.db_path,
        redis_client=connection.redis_client,
        context_logger=connection.context_logger,
        embedding_function=embedding_function,
        cache_ttl=cache_ttl,
        lock=connection.get_lock()
    )

def create_relation_manager(
    connection,
    cache_ttl: int = 3600
) -> RelationManager:
    """
    Create and initialize a RelationManager instance.
    
    Args:
        connection: KnowledgeGraphConnection instance
        cache_ttl: Time-to-live for cached results in seconds
        
    Returns:
        Initialized RelationManager instance
    """
    logger.debug("Creating RelationManager instance")
    
    return RelationManager(
        conn=connection.get_connection(),
        db_path=connection.db_path,
        redis_client=connection.redis_client,
        context_logger=connection.context_logger,
        cache_ttl=cache_ttl,
        lock=connection.get_lock()
    )

def create_search_manager(
    connection,
    embedding_function: Optional[Callable[[str], List[float]]] = None,
    cache_ttl: int = 3600
) -> SearchManager:
    """
    Create and initialize a SearchManager instance.
    
    Args:
        connection: KnowledgeGraphConnection instance
        embedding_function: Optional function to generate embeddings from text
        cache_ttl: Time-to-live for cached results in seconds
        
    Returns:
        Initialized SearchManager instance
    """
    logger.debug("Creating SearchManager instance")
    
    return SearchManager(
        conn=connection.get_connection(),
        db_path=connection.db_path,
        redis_client=connection.redis_client,
        context_logger=connection.context_logger,
        embedding_function=embedding_function,
        cache_ttl=cache_ttl,
        lock=connection.get_lock()
    )

def create_maintenance_manager(
    connection,
    cache_ttl: int = 3600
) -> MaintenanceManager:
    """
    Create and initialize a MaintenanceManager instance.
    
    Args:
        connection: KnowledgeGraphConnection instance
        cache_ttl: Time-to-live for cached results in seconds
        
    Returns:
        Initialized MaintenanceManager instance
    """
    logger.debug("Creating MaintenanceManager instance")
    
    return MaintenanceManager(
        conn=connection.get_connection(),
        db_path=connection.db_path,
        redis_client=connection.redis_client,
        context_logger=connection.context_logger,
        cache_ttl=cache_ttl,
        lock=connection.get_lock()
    )