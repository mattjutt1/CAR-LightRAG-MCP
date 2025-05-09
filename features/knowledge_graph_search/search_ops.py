"""
Search operations for the Knowledge Graph component.

This module contains functions for searching entities in the Knowledge Graph.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from ...knowledge_graph_core_facade.kg_utils import (
    get_cache_key,
    deserialize_embedding,
    execute_with_retry
)
from ...core.utils.json_utils import deserialize_properties
from ...core.exceptions import KnowledgeGraphError

logger = logging.getLogger("car_mcp.features.knowledge_graph_search.search_ops")

def search_entities(
    conn,
    query: str, 
    entity_type: Optional[str] = None,
    limit: int = 10,
    min_similarity: float = 0.0,
    embedding_function=None,
    redis_client=None,
    cache_ttl: int = 3600,
    context_logger=None
) -> List[Dict[str, Any]]:
    """
    Search for entities by name, type, or content.
    
    Args:
        conn: Database connection
        query: Search query
        entity_type: Optional entity type filter
        limit: Maximum number of results to return
        min_similarity: Minimum similarity score (0.0 to 1.0)
        embedding_function: Optional function to generate embeddings
        redis_client: Optional Redis client for caching
        cache_ttl: Time-to-live for cached results in seconds
        context_logger: Optional logger for context events
        
    Returns:
        List of matching entities with similarity scores
        
    Raises:
        KnowledgeGraphError: If an error occurs during search
    """
    if not query:
        raise ValueError("Search query cannot be empty")
    
    # Check cache first
    if redis_client:
        cache_key = get_cache_key("search_entities", query, entity_type, limit, min_similarity)
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Failed to decode cached search results: {e}")
    
    try:
        # Generate embedding for semantic search if available
        query_embedding = None
        if embedding_function is not None:
            try:
                query_embedding = embedding_function(query)
            except Exception as e:
                logger.warning(f"Failed to generate embedding for search query: {e}")
        
        cursor = conn.cursor()
        
        # Prepare query parameters
        params = []
        type_filter = ""
        if entity_type:
            type_filter = "AND entity_type = ?"
            params.append(entity_type)
        
        # Start with a text-based search
        execute_with_retry(
            cursor,
            f"""
            SELECT id, name, entity_type, embedding, created_at, updated_at, properties
            FROM entities
            WHERE (name LIKE ? OR name LIKE ? OR name LIKE ?) {type_filter}
            ORDER BY 
                CASE 
                    WHEN name LIKE ? THEN 1  -- Exact match
                    WHEN name LIKE ? THEN 2  -- Starts with
                    WHEN name LIKE ? THEN 3  -- Contains
                    ELSE 4
                END
            LIMIT ?
            """,
            [
                query,              # Exact match
                f"{query}%",        # Starts with
                f"%{query}%",       # Contains
                query,              # For ORDER BY
                f"{query}%",        # For ORDER BY
                f"%{query}%",       # For ORDER BY
                *params,
                limit
            ]
        )
        
        results = []
        entities_to_check = []
        
        # Process initial text-based results
        for row in cursor.fetchall():
            # Parse embedding
            embedding = deserialize_embedding(row['embedding'])
            
            # Parse properties
            properties = deserialize_properties(row['properties'])
            
            entities_to_check.append({
                "id": row['id'],
                "name": row['name'],
                "entity_type": row['entity_type'],
                "embedding": embedding,
                "created_at": row['created_at'],
                "updated_at": row['updated_at'],
                "properties": properties
            })
        
        # If we have semantic search capabilities and fewer than limit results, try semantic search
        if query_embedding and len(entities_to_check) < limit:
            # This is a simplified semantic search approach
            # In a real implementation, you might want to use a dedicated vector search library
            execute_with_retry(
                cursor,
                f"""
                SELECT id, name, entity_type, embedding, created_at, updated_at, properties
                FROM entities
                WHERE embedding IS NOT NULL {type_filter}
                LIMIT ?
                """,
                [*([entity_type] if entity_type else []), limit * 5]  # Get more candidates for semantic filtering
            )
            
            for row in cursor.fetchall():
                # Skip if already in results
                if any(e["id"] == row['id'] for e in entities_to_check):
                    continue
                
                # Parse embedding
                embedding = deserialize_embedding(row['embedding'])
                
                # Parse properties
                properties = deserialize_properties(row['properties'])
                
                entities_to_check.append({
                    "id": row['id'],
                    "name": row['name'],
                    "entity_type": row['entity_type'],
                    "embedding": embedding,
                    "created_at": row['created_at'],
                    "updated_at": row['updated_at'],
                    "properties": properties
                })
        
        # Calculate similarity scores
        for entity in entities_to_check:
            similarity = 0.0
            
            if query_embedding and entity["embedding"]:
                # Dot product similarity
                dot_product = sum(q * e for q, e in zip(query_embedding, entity["embedding"]))
                
                # Normalization (assuming embeddings are already normalized)
                similarity = max(0.0, min(1.0, dot_product))
            else:
                # Text-based similarity
                name = entity["name"].lower()
                q = query.lower()
                if name == q:
                    similarity = 1.0
                elif name.startswith(q):
                    similarity = 0.8
                elif q in name:
                    similarity = 0.6
                else:
                    similarity = 0.4
            
            # Skip entries below minimum similarity
            if similarity < min_similarity:
                continue
            
            # Add to results
            entity_copy = entity.copy()
            entity_copy.pop("embedding", None)  # Remove large embedding from results
            entity_copy["similarity"] = similarity
            results.append(entity_copy)
        
        # Sort by similarity score and limit results
        results.sort(key=lambda x: x["similarity"], reverse=True)
        results = results[:limit]
        
        # Add observation counts
        for result in results:
            execute_with_retry(
                cursor,
                "SELECT COUNT(*) FROM observations WHERE entity_id = ?",
                (result["id"],)
            )
            result["observation_count"] = cursor.fetchone()[0]
        
        # Cache results
        if redis_client:
            logger.debug(f"Attempting to set cache for search_entities ('{query}', type: {entity_type}, limit: {limit}, min_sim: {min_similarity})")
            try:
                # cache_key was defined at line 58 if redis_client was initially true.
                if cache_key is None:
                    logger.error(f"CRITICAL: cache_key is None for search_entities '{query}' before set. Recalculating.")
                    cache_key = get_cache_key("search_entities", query, entity_type, limit, min_similarity)
                
                redis_client.set(cache_key, json.dumps(results), ex=cache_ttl) # Changed from setex
                logger.debug(f"Successfully set cache for search_entities with key {cache_key}")
            except Exception as e:
                logger.warning(f"Failed to cache search results for '{query}' (key: {cache_key}): {e}", exc_info=True)
        else:
            logger.debug(f"No redis_client, skipping cache set for search_entities '{query}'")

        if context_logger:
            context_logger.log_event(
                "Entity Search",
                {
                    "query": query,
                    "entity_type": entity_type,
                    "result_count": len(results)
                }
            )
        
        return results
        
    except Exception as e:
        error_msg = f"Error searching entities: {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event(
                "Entity Search Error",
                {"query": query, "entity_type": entity_type, "error": error_msg}
            )
        raise KnowledgeGraphError(error_msg) from e