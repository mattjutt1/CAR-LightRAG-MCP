"""
Consolidated CRUD (Create, Read, Update, Delete) operations for Entities
in the Knowledge Graph component.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Adjusted imports based on the new project structure
from ...knowledge_graph_core_facade.kg_models_all import Entity
from ...knowledge_graph_core_facade.kg_utils import (
    invalidate_cache,
    serialize_embedding,
    execute_with_retry,
    get_cache_key,
    deserialize_embedding
)
from ...core.utils.json_utils import serialize_properties, deserialize_properties
from ...core.exceptions import KnowledgeGraphError

logger = logging.getLogger("car_mcp.features.knowledge_graph_entities.ops_entity_crud")

# --- Content from create.py ---
def create_entity(
    conn,
    name: str, 
    entity_type: str,
    embedding_function=None,
    embedding: Optional[List[float]] = None,
    properties: Optional[Dict[str, Any]] = None,
    redis_client=None,
    context_logger=None,
    cache_ttl: Optional[int] = None 
) -> str:
    """
    Create a new entity in the knowledge graph.
    (Docstring from original create.py)
    """
    if not name.strip() or not entity_type.strip(): # Use strip for validation
        raise ValueError("Entity name and type cannot be empty")
    
    if embedding is None and embedding_function is not None:
        try:
            embedding = embedding_function(name)
        except Exception as e:
            logger.warning(f"Failed to generate embedding for entity {name}: {e}")
    
    entity = Entity(
        name=name,
        entity_type=entity_type,
        embedding=embedding,
        properties=properties or {}
    )
    
    try:
        cursor = conn.cursor()
        
        execute_with_retry(
            cursor,
            "SELECT id FROM entities WHERE name = ? AND entity_type = ?", # Check name and type
            (name, entity_type) # Pass both to query
        )
        existing = cursor.fetchone()
        if existing:
            logger.info(f"Entity with name '{name}' and type '{entity_type}' already exists, returning existing ID") # Updated log
            return existing[0]
        
        embedding_json = serialize_embedding(embedding) if embedding else None
        properties_json = serialize_properties(entity.properties)
        
        execute_with_retry(
            cursor,
            """
            INSERT INTO entities 
            (id, name, entity_type, embedding, created_at, updated_at, properties)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity.id,
                entity.name,
                entity.entity_type,
                embedding_json,
                entity.created_at.isoformat(),
                entity.updated_at.isoformat(),
                properties_json
            )
        )
        
        conn.commit()
        
        if context_logger:
            context_logger.log_event(
                "Entity Created",
                {
                    "id": entity.id,
                    "name": entity.name,
                    "entity_type": entity.entity_type
                }
            )
        
        if redis_client:
            try:
                entity_cache_key = get_cache_key("get_entity", entity.id)
                entity_data_for_cache = entity.model_dump_json() if hasattr(entity, 'model_dump_json') else json.dumps(entity.to_dict())
                effective_cache_ttl = cache_ttl if cache_ttl is not None else 3600
                redis_client.set(
                    entity_cache_key,
                    entity_data_for_cache,
                    ex=effective_cache_ttl
                )
                logger.debug(f"Cached new entity {entity.id} with TTL {effective_cache_ttl} after creation.")
            except Exception as e_cache:
                logger.warning(f"Failed to cache newly created entity {entity.id}: {e_cache}")

            invalidate_cache(redis_client, "kg:get_entity_by_name*")
            invalidate_cache(redis_client, "kg:search_entities*")
        
        logger.info(f"Created entity '{name}' with ID: {entity.id}")
        return entity.id
    
    except Exception as e:
        conn.rollback()
        error_msg = f"Error creating entity '{name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        if context_logger:
            context_logger.log_event(
                "Entity Creation Error",
                {"name": name, "entity_type": entity_type, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e

# --- Content from read.py ---
def get_entity(
    conn,
    entity_id: str,
    redis_client=None,
    cache_ttl: int = 3600,
    context_logger=None
) -> Optional[Entity]:
    """
    Get an entity by its ID.
    (Docstring from original read.py)
    """
    logger.debug(f"ops_entity_crud.get_entity called for {entity_id}. Received redis_client ID: {id(redis_client)}")
    if not entity_id:
        raise ValueError("Entity ID cannot be empty")
    
    cached_data = None 
    cache_key = None 
    if redis_client:
        cache_key = get_cache_key("get_entity", entity_id)
        try:
            cached_data = redis_client.get(cache_key)
        except Exception as e_get_cache:
            logger.warning(f"Error getting entity {entity_id} from cache (key: {cache_key}): {e_get_cache}", exc_info=True)
            cached_data = None 

        if cached_data:
            try:
                entity_dict = json.loads(cached_data)
                return Entity(**entity_dict) if hasattr(Entity, 'model_validate') else Entity.from_dict(entity_dict) 
            except Exception as e_decode:
                logger.warning(f"Failed to decode cached entity {entity_id} (key: {cache_key}): {e_decode}. Falling through to DB.", exc_info=True)
                cached_data = None 
    
    logger.debug(f"Cache miss or error for entity {entity_id} (cache_key: {cache_key}, cached_data is {'None' if cached_data is None else 'Present but decode failed/skipped'}). Fetching from DB.")
    try:
        cursor = conn.cursor()
        execute_with_retry(
            cursor,
            """
            SELECT id, name, entity_type, embedding, created_at, updated_at, properties
            FROM entities
            WHERE id = ?
            """,
            (entity_id,)
        )
        
        row = cursor.fetchone()
        if not row:
            return None
        
        embedding_val = deserialize_embedding(row['embedding'])
        properties_val = deserialize_properties(row['properties'])
        
        entity = Entity(
            id=row['id'],
            name=row['name'],
            entity_type=row['entity_type'],
            embedding=embedding_val,
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at']),
            properties=properties_val
        )
        
        entity = _add_observations_to_entity(conn, entity)
        logger.debug(f"After _add_observations_to_entity for {entity_id}, entity is {'None' if entity is None else 'Exists'}")
        
        logger.debug(f"Before final cache set logic for {entity_id}: redis_client ID is {id(redis_client)}, (redis_client is not None) is {redis_client is not None}")
        try:
            if redis_client: 
                logger.debug(f"Cache set logic: redis_client is present for {entity_id}. ID: {id(redis_client)}")
                if cache_key is None: 
                     logger.error(f"CRITICAL: cache_key is None for {entity_id} before attempting set, though redis_client is present. Recalculating.")
                     cache_key = get_cache_key("get_entity", entity_id)

                entity_data_for_cache = entity.model_dump_json() if hasattr(entity, 'model_dump_json') else json.dumps(entity.to_dict())
                logger.debug(f"Attempting to set cache for {entity_id} with key {cache_key}. Data (first 100): {entity_data_for_cache[:100]}...")
                redis_client.set(cache_key, entity_data_for_cache, ex=cache_ttl)
                logger.debug(f"Successfully set cache for {entity_id} with key {cache_key}.")
            else:
                logger.debug(f"No redis_client (final check was False), skipping cache set for entity {entity_id} after DB fetch.")
        except Exception as e_cache_set_block: 
            logger.error(f"ERROR in cache set logic block for {entity_id}: {e_cache_set_block}", exc_info=True)
        return entity
        
    except Exception as e:
        error_msg = f"Error retrieving entity with ID '{entity_id}': {str(e)}"
        logger.error(error_msg, exc_info=True) # Already has exc_info=True, good.
        if context_logger:
            context_logger.log_event(
                "Entity Retrieval Error",
                {"id": entity_id, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e

def get_entity_by_name(
    conn,
    name: str,
    redis_client=None,
    cache_ttl: int = 3600,
    context_logger=None
) -> Optional[Entity]:
    """
    Get an entity by its name.
    (Docstring from original read.py)
    """
    logger.debug(f"ops_entity_crud.get_entity_by_name called for {name}. Received redis_client ID: {id(redis_client)}")
    if not name:
        raise ValueError("Entity name cannot be empty")
    
    cached_data = None 
    cache_key = None 
    if redis_client:
        cache_key = get_cache_key("get_entity_by_name", name)
        try:
            cached_data = redis_client.get(cache_key)
        except Exception as e_get_cache:
            logger.warning(f"Error getting entity by name {name} from cache (key: {cache_key}): {e_get_cache}", exc_info=True)
            cached_data = None

        if cached_data:
            try:
                entity_dict = json.loads(cached_data)
                return Entity(**entity_dict) if hasattr(Entity, 'model_validate') else Entity.from_dict(entity_dict)
            except Exception as e_decode:
                logger.warning(f"Failed to decode cached entity by name {name} (key: {cache_key}): {e_decode}. Falling through to DB.", exc_info=True)
                cached_data = None
    
    logger.debug(f"Cache miss or error for entity by name {name} (cache_key: {cache_key}, cached_data is {'None' if cached_data is None else 'Present but decode failed/skipped'}). Fetching from DB.")
    try:
        cursor = conn.cursor()
        execute_with_retry(
            cursor,
            """
            SELECT id, name, entity_type, embedding, created_at, updated_at, properties
            FROM entities
            WHERE name = ?
            """,
            (name,)
        )
        
        row = cursor.fetchone()
        if not row:
            return None
        
        embedding_val = deserialize_embedding(row['embedding'])
        properties_val = deserialize_properties(row['properties'])
        
        entity = Entity(
            id=row['id'],
            name=row['name'],
            entity_type=row['entity_type'],
            embedding=embedding_val,
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at']),
            properties=properties_val
        )
        
        entity = _add_observations_to_entity(conn, entity)
        logger.debug(f"After _add_observations_to_entity for {name}, entity is {'None' if entity is None else 'Exists'}")

        logger.debug(f"Before final cache set logic for {name}: redis_client ID is {id(redis_client)}, (redis_client is not None) is {redis_client is not None}")
        try:
            if redis_client:
                logger.debug(f"Cache set logic: redis_client is present for {name}. ID: {id(redis_client)}")
                if cache_key is None: 
                    logger.error(f"CRITICAL: cache_key is None for {name} before attempting set, though redis_client is present. Recalculating.")
                    cache_key = get_cache_key("get_entity_by_name", name)
                
                entity_data_for_cache = entity.model_dump_json() if hasattr(entity, 'model_dump_json') else json.dumps(entity.to_dict())
                logger.debug(f"Attempting to set cache for {name} with key {cache_key}. Data (first 100): {entity_data_for_cache[:100]}...")
                redis_client.set(cache_key, entity_data_for_cache, ex=cache_ttl)
                logger.debug(f"Successfully set cache for entity by name {name} with key {cache_key}.")
            else:
                logger.debug(f"No redis_client (final check was False), skipping cache set for entity by name {name} after DB fetch.")
        except Exception as e_cache_set_block:
            logger.error(f"ERROR in cache set logic block for entity by name {name}: {e_cache_set_block}", exc_info=True)
        return entity
        
    except Exception as e:
        error_msg = f"Error retrieving entity with name '{name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        if context_logger:
            context_logger.log_event(
                "Entity Retrieval Error",
                {"name": name, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e

def _add_observations_to_entity(conn, entity: Entity) -> Entity:
    """
    Add observations to an entity object.
    (Docstring from original read.py)
    """
    try:
        cursor = conn.cursor()
        execute_with_retry(
            cursor,
            """
            SELECT observation
            FROM observations
            WHERE entity_id = ?
            ORDER BY created_at DESC
            """,
            (entity.id,)
        )
        
        observations_data = [row['observation'] for row in cursor.fetchall()]
        
        if entity.properties is None: 
            entity.properties = {}
            
        entity.properties["observations"] = observations_data
        entity.updated_at = datetime.now() 

        return entity
        
    except Exception as e:
        logger.warning(f"Error adding observations to entity properties for {entity.id if entity else 'None'}: {e}", exc_info=True)
        return entity

# --- Content from update.py ---
def update_entity(
    conn,
    entity_id: str, 
    name: Optional[str] = None,
    entity_type: Optional[str] = None,
    embedding: Optional[List[float]] = None,
    properties: Optional[Dict[str, Any]] = None,
    redis_client=None,
    context_logger=None
) -> bool:
    """
    Update an existing entity.
    (Docstring from original update.py)
    """
    if not entity_id:
        raise ValueError("Entity ID cannot be empty")
    
    try:
        cursor = conn.cursor()
        execute_with_retry(
            cursor,
            "SELECT id FROM entities WHERE id = ?",
            (entity_id,)
        )
        if not cursor.fetchone():
            return False
        
        updates = []
        params: List[Any] = [] 
        
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        
        if entity_type is not None:
            updates.append("entity_type = ?")
            params.append(entity_type)
        
        if embedding is not None:
            updates.append("embedding = ?")
            params.append(serialize_embedding(embedding))
        
        if properties is not None:
            updates.append("properties = ?")
            params.append(serialize_properties(properties))
        
        if not updates: 
            pass

        updates.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        
        params.append(entity_id) 
        
        execute_with_retry(
            cursor,
            f"UPDATE entities SET {', '.join(updates)} WHERE id = ?",
            params
        )
            
        conn.commit()
            
        if redis_client:
            invalidate_cache(redis_client, "kg:get_entity*") 
            invalidate_cache(redis_client, "kg:get_entity_by_name*")
            invalidate_cache(redis_client, "kg:search_entities*")
            
        if context_logger:
            context_logger.log_event(
                "Entity Updated",
                {
                    "id": entity_id,
                    "fields_updated": [u.split(" = ")[0] for u in updates if "updated_at" not in u] 
                }
            )
            
        logger.info(f"Updated entity with ID: {entity_id}")
        return True
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error updating entity '{entity_id}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        if context_logger:
            context_logger.log_event(
                "Entity Update Error",
                {"id": entity_id, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e

# --- Content from delete.py ---
def delete_entity(
    conn,
    entity_id: str,
    redis_client=None,
    context_logger=None
) -> bool:
    """
    Delete an entity and all its relations and observations.
    (Docstring from original delete.py)
    """
    if not entity_id:
        raise ValueError("Entity ID cannot be empty")
    
    try:
        entity = get_entity(conn, entity_id, redis_client=redis_client, context_logger=context_logger)
        if not entity:
            return False
        
        cursor = conn.cursor()
        execute_with_retry(
            cursor,
            "DELETE FROM entities WHERE id = ?",
            (entity_id,)
        )
        
        conn.commit()
        
        if redis_client:
            invalidate_cache(redis_client, "kg:get_entity*")
            invalidate_cache(redis_client, "kg:get_entity_by_name*")
            invalidate_cache(redis_client, "kg:search_entities*")
            invalidate_cache(redis_client, "kg:get_relations*") 
        
        if context_logger:
            context_logger.log_event(
                "Entity Deleted",
                {"id": entity_id, "name": entity.name} 
            )
        
        logger.info(f"Deleted entity with ID: {entity_id}")
        return True
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error deleting entity '{entity_id}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        if context_logger:
            context_logger.log_event(
                "Entity Deletion Error",
                {"id": entity_id, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e