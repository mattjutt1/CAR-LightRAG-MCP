"""
Consolidated CRUD (Create, Read, Update, Delete) operations for Observations
in the Knowledge Graph component. 'Update' for observations is typically handled
by deleting and re-adding, so only Add, Read, Delete are included.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Adjusted imports based on the new project structure
from ...knowledge_graph_core_facade.kg_models_all import Observation, Entity
from ...knowledge_graph_core_facade.kg_utils import (
    serialize_embedding,
    execute_with_retry,
    invalidate_cache,
    get_cache_key,
    deserialize_embedding
)
from ...core.utils.json_utils import serialize_properties, deserialize_properties
from ...core.exceptions import KnowledgeGraphError, EntityNotFoundError

logger = logging.getLogger("car_mcp.features.knowledge_graph_observations.ops_observation_crud")

# --- Content from car_mcp/knowledge_graph/operations/observation/add.py ---
def add_observation(
    conn,
    entity_id: str, 
    observation: str,
    embedding_function=None,
    embedding: Optional[List[float]] = None,
    properties: Optional[Dict[str, Any]] = None,
    redis_client=None,
    context_logger=None
) -> str:
    """
    Add an observation to an entity.
    (Docstring from original add.py)
    """
    if not entity_id or not observation:
        raise ValueError("Entity ID and observation cannot be empty")
    
    if embedding is None and embedding_function is not None:
        try:
            embedding = embedding_function(observation)
        except Exception as e:
            logger.warning(f"Failed to generate embedding for observation: {e}")
    
    cursor = conn.cursor() # Moved cursor init earlier
    execute_with_retry(cursor, "SELECT name FROM entities WHERE id = ?", (entity_id,))
    entity_row = cursor.fetchone()
    if not entity_row:
        raise EntityNotFoundError(f"Entity with ID '{entity_id}' not found")
    entity_name = entity_row[0]
    
    obs = Observation(
        entity_id=entity_id,
        observation=observation,
        embedding=embedding,
        properties=properties or {}
    )
    
    try:
        embedding_json = serialize_embedding(embedding) if embedding else None
        properties_json = serialize_properties(obs.properties)
        
        execute_with_retry(
            cursor,
            """
            INSERT INTO observations 
            (id, entity_id, observation, embedding, created_at, properties)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                obs.id, obs.entity_id, obs.observation, embedding_json,
                obs.created_at.isoformat(), properties_json
            )
        )
        
        execute_with_retry(
            cursor,
            "UPDATE entities SET updated_at = ? WHERE id = ?",
            (datetime.now().isoformat(), entity_id)
        )
        
        conn.commit()
        
        if context_logger:
            context_logger.log_event(
                "Observation Added",
                {"id": obs.id, "entity_id": entity_id, "entity_name": entity_name}
            )
        
        if redis_client:
            invalidate_cache(redis_client, "kg:get_entity*")
            invalidate_cache(redis_client, "kg:get_entity_by_name*")
            invalidate_cache(redis_client, "kg:get_observations*") # Added this specific cache invalidation
        
        logger.info(f"Added observation to entity '{entity_name}' (ID: {entity_id})")
        return obs.id
        
    except EntityNotFoundError: # Should be caught before try block if entity check is robust
        raise
    except Exception as e:
        conn.rollback()
        error_msg = f"Error adding observation to entity '{entity_id}': {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Observation Addition Error", {"entity_id": entity_id, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/observation/read.py ---
def get_observations(
    conn,
    entity_id: str, 
    limit: int = 100,
    redis_client=None,
    cache_ttl: int = 3600,
    context_logger=None
) -> List[Observation]:
    """
    Get observations for an entity.
    (Docstring from original read.py)
    """
    if not entity_id:
        raise ValueError("Entity ID cannot be empty")
    
    if redis_client:
        cache_key = get_cache_key("get_observations", entity_id, limit)
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                observations_dict = json.loads(cached_data)
                # Assuming Observation.from_dict or Pydantic parsing
                return [Observation(**obs_data) if hasattr(Observation, 'model_validate') else Observation.from_dict(obs_data) for obs_data in observations_dict]
            except Exception as e:
                logger.warning(f"Failed to decode cached observations: {e}")
    
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, "SELECT id FROM entities WHERE id = ?", (entity_id,))
        if not cursor.fetchone():
            raise EntityNotFoundError(f"Entity with ID '{entity_id}' not found")
        
        execute_with_retry(
            cursor,
            """
            SELECT id, entity_id, observation, embedding, created_at, properties
            FROM observations
            WHERE entity_id = ? ORDER BY created_at DESC LIMIT ?
            """,
            (entity_id, limit)
        )
        
        observations_list = []
        for row in cursor.fetchall():
            embedding_val = deserialize_embedding(row['embedding'])
            properties_val = deserialize_properties(row['properties'])
            obs = Observation(
                id=row['id'], entity_id=row['entity_id'], observation=row['observation'],
                embedding=embedding_val, created_at=datetime.fromisoformat(row['created_at']),
                properties=properties_val
            )
            observations_list.append(obs)
        
        if redis_client:
            logger.debug(f"Attempting to set cache for get_observations (entity: {entity_id}, limit: {limit})")
            try:
                # cache_key was defined at line 130 if redis_client was initially true.
                if cache_key is None:
                    logger.error(f"CRITICAL: cache_key is None for get_observations {entity_id} before set. Recalculating.")
                    cache_key = get_cache_key("get_observations", entity_id, limit)

                obs_data_for_cache = [o.model_dump() if hasattr(o, 'model_dump') else o.to_dict() for o in observations_list]
                redis_client.set(cache_key, json.dumps(obs_data_for_cache), ex=cache_ttl) # Changed from setex
                logger.debug(f"Successfully set cache for get_observations with key {cache_key}")
            except Exception as e:
                logger.warning(f"Failed to cache observations for {entity_id} (key: {cache_key}): {e}", exc_info=True)
        else:
            logger.debug(f"No redis_client, skipping cache set for get_observations {entity_id}")
            
        return observations_list
        
    except EntityNotFoundError:
        raise
    except Exception as e:
        error_msg = f"Error retrieving observations for entity '{entity_id}': {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Observation Retrieval Error", {"entity_id": entity_id, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/observation/delete.py ---
def delete_observation(
    conn,
    observation_id: str,
    redis_client=None,
    context_logger=None
) -> bool:
    """
    Delete an observation.
    (Docstring from original delete.py)
    """
    if not observation_id:
        raise ValueError("Observation ID cannot be empty")
    
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, "SELECT entity_id FROM observations WHERE id = ?", (observation_id,))
        row = cursor.fetchone()
        if not row:
            return False
        entity_id = row['entity_id']
        
        execute_with_retry(cursor, "DELETE FROM observations WHERE id = ?", (observation_id,))
        
        execute_with_retry(
            cursor,
            "UPDATE entities SET updated_at = ? WHERE id = ?",
            (datetime.now().isoformat(), entity_id)
        )
        
        conn.commit()
        
        if redis_client:
            invalidate_cache(redis_client, "kg:get_entity*") # Entity was updated
            invalidate_cache(redis_client, "kg:get_entity_by_name*")
            invalidate_cache(redis_client, "kg:get_observations*") # Observations for entity changed
        
        if context_logger:
            context_logger.log_event("Observation Deleted", {"id": observation_id, "entity_id": entity_id})
        
        logger.info(f"Deleted observation with ID: {observation_id}")
        return True
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error deleting observation '{observation_id}': {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Observation Deletion Error", {"id": observation_id, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e