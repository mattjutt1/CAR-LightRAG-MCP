"""
Knowledge Graph Observation API Module.

This module provides a clean API for observation operations in the Knowledge Graph.
It's part of the facade pattern implementation to maintain backward compatibility
while refactoring the monolithic KnowledgeGraph class into multiple focused modules.
"""

import logging
from typing import Dict, List, Optional, Any

from ..features.knowledge_graph_observations.observation_manager import ObservationManager
from ..knowledge_graph_core_facade.kg_models_all import Observation
from ..core.exceptions import KnowledgeGraphError, EntityNotFoundError

# Configure logging
logger = logging.getLogger("car_mcp.knowledge_graph_core_facade.kg_observation_api")

class KnowledgeGraphObservationAPI:
    """
    API for observation operations in the Knowledge Graph.
    
    This class provides a clean interface for observation operations while delegating
    the actual implementation to the ObservationManager. It's part of the facade pattern
    to maintain backward compatibility during the refactoring of the monolithic
    KnowledgeGraph class.
    """
    
    def __init__(self, observation_manager: ObservationManager):
        """
        Initialize the Knowledge Graph Observation API.
        
        Args:
            observation_manager: ObservationManager instance to delegate operations to
        """
        self._observation_manager = observation_manager
        logger.debug("KnowledgeGraphObservationAPI initialized")
    
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
        try:
            logger.debug(f"Adding observation to entity with ID: {entity_id}")
            observation_id = self._observation_manager.add_observation(
                entity_id=entity_id,
                observation=observation,
                embedding=embedding,
                properties=properties
            )
            logger.debug(f"Observation added with ID: {observation_id}")
            return observation_id
        except EntityNotFoundError:
            # Re-raise EntityNotFoundError without wrapping it
            raise
        except Exception as e:
            error_msg = f"Error adding observation to entity '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
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
        try:
            logger.debug(f"Getting observations for entity with ID: {entity_id}, limit: {limit}")
            observations = self._observation_manager.get_observations(
                entity_id=entity_id,
                limit=limit
            )
            logger.debug(f"Retrieved {len(observations)} observations for entity with ID: {entity_id}")
            return observations
        except EntityNotFoundError:
            # Re-raise EntityNotFoundError without wrapping it
            raise
        except Exception as e:
            error_msg = f"Error retrieving observations for entity '{entity_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e
    
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
        try:
            logger.debug(f"Deleting observation with ID: {observation_id}")
            result = self._observation_manager.delete_observation(
                observation_id=observation_id
            )
            
            if result:
                logger.debug(f"Observation with ID {observation_id} deleted successfully")
            else:
                logger.warning(f"Observation with ID {observation_id} not found for deletion")
                
            return result
        except Exception as e:
            error_msg = f"Error deleting observation with ID '{observation_id}': {str(e)}"
            logger.error(error_msg)
            raise KnowledgeGraphError(error_msg) from e