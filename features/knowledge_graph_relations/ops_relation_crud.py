"""
Consolidated CRUD (Create, Read, Update, Delete) operations for Relations
in the Knowledge Graph component.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Adjusted imports based on the new project structure
from ...knowledge_graph_core_facade.kg_models_all import Relation # Assuming Relation model is needed
from ...knowledge_graph_core_facade.kg_utils import (
    execute_with_retry,
    invalidate_cache,
    get_cache_key
)
from ...core.utils.json_utils import serialize_properties, deserialize_properties
from ...core.exceptions import KnowledgeGraphError, EntityNotFoundError

logger = logging.getLogger("car_mcp.features.knowledge_graph_relations.ops_relation_crud")

# --- Content from car_mcp/knowledge_graph/operations/relation/create.py ---
def create_relation(
    conn,
    from_entity_id: str, 
    to_entity_id: str, 
    relation_type: str,
    confidence: float = 1.0,
    properties: Optional[Dict[str, Any]] = None,
    redis_client=None,
    context_logger=None
) -> str:
    """
    Create a relation between two entities.
    (Docstring from original create.py)
    """
    if not from_entity_id or not to_entity_id or not relation_type:
        raise ValueError("Source entity ID, target entity ID, and relation type cannot be empty")
    
    if confidence < 0.0 or confidence > 1.0:
        raise ValueError("Confidence score must be between 0.0 and 1.0")
    
    try:
        cursor = conn.cursor()
        
        execute_with_retry(
            cursor,
            "SELECT name FROM entities WHERE id = ?",
            (from_entity_id,)
        )
        from_entity_row = cursor.fetchone()
        if not from_entity_row:
            raise EntityNotFoundError(f"Source entity with ID '{from_entity_id}' not found")
        
        execute_with_retry(
            cursor,
            "SELECT name FROM entities WHERE id = ?",
            (to_entity_id,)
        )
        to_entity_row = cursor.fetchone()
        if not to_entity_row:
            raise EntityNotFoundError(f"Target entity with ID '{to_entity_id}' not found")
        
        from_entity_name = from_entity_row[0]
        to_entity_name = to_entity_row[0]
        
        execute_with_retry(
            cursor,
            "SELECT id FROM relations WHERE from_entity_id = ? AND to_entity_id = ? AND relation_type = ?",
            (from_entity_id, to_entity_id, relation_type)
        )
        existing = cursor.fetchone()
        if existing:
            logger.info(f"Relation of type '{relation_type}' already exists between these entities")
            return existing[0]
        
        relation = Relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type=relation_type,
            confidence=confidence,
            properties=properties or {}
        )
        
        properties_json = serialize_properties(relation.properties)
        
        execute_with_retry(
            cursor,
            """
            INSERT INTO relations 
            (id, from_entity_id, to_entity_id, relation_type, confidence, created_at, properties)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                relation.id,
                relation.from_entity_id,
                relation.to_entity_id,
                relation.relation_type,
                relation.confidence,
                relation.created_at.isoformat(),
                properties_json
            )
        )
        
        now = datetime.now().isoformat()
        execute_with_retry(cursor, "UPDATE entities SET updated_at = ? WHERE id = ?", (now, from_entity_id))
        execute_with_retry(cursor, "UPDATE entities SET updated_at = ? WHERE id = ?", (now, to_entity_id))
        
        conn.commit()
        
        if context_logger:
            context_logger.log_event(
                "Relation Created",
                {
                    "id": relation.id, "from_entity": from_entity_name, "to_entity": to_entity_name,
                    "relation_type": relation_type, "confidence": confidence
                }
            )
        
        if redis_client:
            invalidate_cache(redis_client, "kg:get_relations*")
            invalidate_cache(redis_client, "kg:get_entity*")
        
        logger.info(f"Created relation of type '{relation_type}' from '{from_entity_name}' to '{to_entity_name}'")
        return relation.id
        
    except EntityNotFoundError:
        raise
    except Exception as e:
        conn.rollback()
        error_msg = f"Error creating relation: {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event(
                "Relation Creation Error",
                {"from_entity_id": from_entity_id, "to_entity_id": to_entity_id, "relation_type": relation_type, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/relation/read.py ---
def get_relations(
    conn,
    entity_id: str, 
    direction: str = "both", 
    relation_type: Optional[str] = None,
    redis_client=None,
    cache_ttl: int = 3600,
    context_logger=None
) -> List[Dict[str, Any]]:
    """
    Get relations for an entity.
    (Docstring from original read.py)
    """
    if not entity_id:
        raise ValueError("Entity ID cannot be empty")
    if direction not in ["outgoing", "incoming", "both"]:
        raise ValueError("Direction must be 'outgoing', 'incoming', or 'both'")
    
    if redis_client:
        cache_key = get_cache_key("get_relations", entity_id, direction, relation_type)
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Failed to decode cached relations: {e}")
    
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, "SELECT name FROM entities WHERE id = ?", (entity_id,))
        entity_row = cursor.fetchone()
        if not entity_row:
            raise EntityNotFoundError(f"Entity with ID '{entity_id}' not found")
        
        entity_name = entity_row[0]
        relations_data: List[Dict[str, Any]] = [] # Explicitly type for clarity
        
        base_query_outgoing = """
            SELECT r.id, r.from_entity_id, r.to_entity_id, r.relation_type, r.confidence, 
                   r.created_at, r.properties, e.name as to_entity_name
            FROM relations r JOIN entities e ON r.to_entity_id = e.id
            WHERE r.from_entity_id = ?
        """
        base_query_incoming = """
            SELECT r.id, r.from_entity_id, r.to_entity_id, r.relation_type, r.confidence, 
                   r.created_at, r.properties, e.name as from_entity_name
            FROM relations r JOIN entities e ON r.from_entity_id = e.id
            WHERE r.to_entity_id = ?
        """

        if direction in ["outgoing", "both"]:
            query = base_query_outgoing
            params = [entity_id]
            if relation_type:
                query += " AND r.relation_type = ?"
                params.append(relation_type)
            execute_with_retry(cursor, query, tuple(params))
            for row in cursor.fetchall():
                properties = deserialize_properties(row['properties'])
                relations_data.append({
                    "id": row['id'], "from_entity_id": row['from_entity_id'], "from_entity_name": entity_name,
                    "to_entity_id": row['to_entity_id'], "to_entity_name": row['to_entity_name'],
                    "relation_type": row['relation_type'], "confidence": row['confidence'],
                    "direction": "outgoing", "created_at": row['created_at'], "properties": properties
                })
        
        if direction in ["incoming", "both"]:
            query = base_query_incoming
            params = [entity_id]
            if relation_type:
                query += " AND r.relation_type = ?"
                params.append(relation_type)
            execute_with_retry(cursor, query, tuple(params))
            for row in cursor.fetchall():
                properties = deserialize_properties(row['properties'])
                relations_data.append({
                    "id": row['id'], "from_entity_id": row['from_entity_id'], "from_entity_name": row['from_entity_name'],
                    "to_entity_id": row['to_entity_id'], "to_entity_name": entity_name,
                    "relation_type": row['relation_type'], "confidence": row['confidence'],
                    "direction": "incoming", "created_at": row['created_at'], "properties": properties
                })
        
        if redis_client:
            logger.debug(f"Attempting to set cache for get_relations (entity: {entity_id}, dir: {direction}, type: {relation_type})")
            try:
                # cache_key was defined at line 161 if redis_client was initially true.
                if cache_key is None: # Should only happen if redis_client was None initially
                    logger.error(f"CRITICAL: cache_key is None for get_relations {entity_id} before set. Recalculating.")
                    cache_key = get_cache_key("get_relations", entity_id, direction, relation_type)

                redis_client.set(cache_key, json.dumps(relations_data), ex=cache_ttl) # Changed from setex
                logger.debug(f"Successfully set cache for get_relations with key {cache_key}")
            except Exception as e:
                logger.warning(f"Failed to cache relations for {entity_id} (key: {cache_key}): {e}", exc_info=True)
        else:
            logger.debug(f"No redis_client, skipping cache set for get_relations {entity_id}")
            
        return relations_data
        
    except EntityNotFoundError:
        raise
    except Exception as e:
        error_msg = f"Error retrieving relations for entity '{entity_id}': {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Relation Retrieval Error", {"entity_id": entity_id, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/relation/delete.py ---
def delete_relation(
    conn,
    relation_id: str,
    redis_client=None,
    context_logger=None
) -> bool:
    """
    Delete a relation.
    (Docstring from original delete.py)
    """
    if not relation_id:
        raise ValueError("Relation ID cannot be empty")
    
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, "SELECT from_entity_id, to_entity_id FROM relations WHERE id = ?", (relation_id,))
        row = cursor.fetchone()
        if not row:
            return False
        
        from_entity_id, to_entity_id = row['from_entity_id'], row['to_entity_id']
        
        execute_with_retry(cursor, "DELETE FROM relations WHERE id = ?", (relation_id,))
        
        now = datetime.now().isoformat()
        execute_with_retry(cursor, "UPDATE entities SET updated_at = ? WHERE id = ?", (now, from_entity_id))
        execute_with_retry(cursor, "UPDATE entities SET updated_at = ? WHERE id = ?", (now, to_entity_id))
        
        conn.commit()
        
        if redis_client:
            invalidate_cache(redis_client, "kg:get_relations*")
        
        if context_logger:
            context_logger.log_event(
                "Relation Deleted",
                {"id": relation_id, "from_entity_id": from_entity_id, "to_entity_id": to_entity_id}
            )
        
        logger.info(f"Deleted relation with ID: {relation_id}")
        return True
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error deleting relation '{relation_id}': {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Relation Deletion Error", {"id": relation_id, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e