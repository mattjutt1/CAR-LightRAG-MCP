"""
Consolidated Maintenance operations for the Knowledge Graph component.
Includes admin, backup, restore, and statistics gathering.
"""

import os
import json
import shutil
import logging
from typing import Dict, Tuple, Any
from datetime import datetime

# Adjusted imports based on the new project structure
from ...knowledge_graph_core_facade.db_handler import get_database_size, get_connection
from ...knowledge_graph_core_facade.kg_utils import execute_with_retry
from ...core.exceptions import KnowledgeGraphError

logger = logging.getLogger("car_mcp.features.knowledge_graph_maintenance.ops_maintenance")

# --- Content from car_mcp/knowledge_graph/operations/maintenance/admin.py ---
def clear_knowledge_graph(
    conn,
    redis_client=None,
    context_logger=None
) -> Dict[str, int]:
    """
    Clear all data from the knowledge graph.
    """
    try:
        cursor = conn.cursor()
        
        entity_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM entities").fetchone()
        entity_count = entity_count_row[0] if entity_count_row else 0
        
        relation_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM relations").fetchone()
        relation_count = relation_count_row[0] if relation_count_row else 0
        
        observation_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM observations").fetchone()
        observation_count = observation_count_row[0] if observation_count_row else 0
        
        execute_with_retry(cursor, "DELETE FROM relations")
        execute_with_retry(cursor, "DELETE FROM observations")
        execute_with_retry(cursor, "DELETE FROM entities")
        conn.commit() # Commit deletions before VACUUM

        # VACUUM should be run outside of a transaction or after committing previous changes.
        # Some SQLite versions might implicitly start a transaction for VACUUM.
        try:
            logger.info("Attempting to VACUUM database.")
            cursor.execute("VACUUM")
            conn.commit() # Ensure VACUUM is committed if it doesn't auto-commit.
            logger.info("Database VACUUM completed.")
        except Exception as vacuum_error:
            logger.warning(f"Could not VACUUM database: {vacuum_error}. Proceeding without vacuum.")

        if redis_client:
            try:
                keys = redis_client.keys("kg:*")
                if keys:
                    redis_client.delete(*keys)
                    logger.debug(f"Invalidated {len(keys)} cache entries")
            except Exception as e:
                logger.warning(f"Error invalidating cache: {str(e)}")
        
        if context_logger:
            context_logger.log_event(
                "Knowledge Graph Cleared",
                {"entity_count": entity_count, "relation_count": relation_count, "observation_count": observation_count}
            )
        
        logger.info(f"Cleared knowledge graph: {entity_count} entities, {relation_count} relations, {observation_count} observations")
        return {"entity_count": entity_count, "relation_count": relation_count, "observation_count": observation_count}
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Error clearing knowledge graph: {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Knowledge Graph Clear Error", {"error": error_msg})
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/maintenance/stats.py ---
def get_knowledge_graph_stats(
    conn,
    db_path: str,
    redis_client=None, # Added for caching
    cache_ttl: int = 3600 # Added for caching
) -> Dict[str, Any]:
    """
    Get statistics about the knowledge graph.
    """
    cache_key = "kg:stats" # Stats cache key is simple
    if redis_client:
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.debug(f"Cache hit for stats with key {cache_key}")
                return json.loads(cached_data)
        except Exception as e_get_cache:
            logger.warning(f"Error getting stats from cache (key: {cache_key}): {e_get_cache}", exc_info=True)
            # Fall through to DB query if cache get fails

    logger.debug(f"Cache miss for stats (key: {cache_key}). Fetching from DB.")
    try:
        cursor = conn.cursor()
        
        entity_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM entities").fetchone()
        entity_count = entity_count_row[0] if entity_count_row else 0
        
        relation_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM relations").fetchone()
        relation_count = relation_count_row[0] if relation_count_row else 0
        
        observation_count_row = execute_with_retry(cursor, "SELECT COUNT(*) FROM observations").fetchone()
        observation_count = observation_count_row[0] if observation_count_row else 0
        
        execute_with_retry(cursor, "SELECT entity_type, COUNT(*) FROM entities GROUP BY entity_type")
        entity_types = {row[0]: row[1] for row in cursor.fetchall()}
        
        execute_with_retry(cursor, "SELECT relation_type, COUNT(*) FROM relations GROUP BY relation_type")
        relation_types = {row[0]: row[1] for row in cursor.fetchall()}
        
        execute_with_retry(
            cursor, 
            "SELECT e.name, COUNT(o.id) FROM entities e JOIN observations o ON e.id = o.entity_id GROUP BY e.id ORDER BY COUNT(o.id) DESC LIMIT 10"
        )
        top_observed_entities = {row[0]: row[1] for row in cursor.fetchall()}
        
        db_size = get_database_size(db_path) # Uses imported get_database_size
        
        stats_data = {
            "timestamp": datetime.now().isoformat(), "entity_count": entity_count, "relation_count": relation_count,
            "observation_count": observation_count, "entity_types": entity_types, "relation_types": relation_types,
            "top_observed_entities": top_observed_entities, "db_path": db_path,
            "db_size_bytes": db_size, "db_size_mb": round(db_size / (1024 * 1024), 2)
        }

        if redis_client:
            logger.debug(f"Attempting to set cache for stats with key {cache_key}")
            try:
                redis_client.set(cache_key, json.dumps(stats_data), ex=cache_ttl)
                logger.debug(f"Successfully set cache for stats with key {cache_key}")
            except Exception as e_set_cache:
                logger.warning(f"Failed to cache stats (key: {cache_key}): {e_set_cache}", exc_info=True)
        else:
            logger.debug(f"No redis_client, skipping cache set for stats (key: {cache_key})")
            
        return stats_data
        
    except Exception as e:
        error_msg = f"Error getting knowledge graph statistics: {str(e)}"
        logger.error(error_msg, exc_info=True) # Added exc_info
        raise KnowledgeGraphError(error_msg) from e

# --- Content from car_mcp/knowledge_graph/operations/maintenance/backup.py ---
def backup_knowledge_graph(
    # conn parameter is tricky here as it needs to be closed for shutil.copy2
    # The service layer should manage the connection lifecycle.
    # For now, assume db_path is sufficient and connection is handled by caller or internally.
    db_path: str, 
    backup_dir: str,
    context_logger=None
) -> Tuple[str, str]:
    """
    Create a backup of the knowledge graph database.
    The caller is responsible for managing database connections before/after backup.
    """
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_backup_file = f"knowledge_graph_{timestamp}.db"
    stats_file = f"knowledge_graph_stats_{timestamp}.json"
    
    db_backup_full_path = os.path.join(backup_dir, db_backup_file)
    stats_full_path = os.path.join(backup_dir, stats_file)
    
    conn_for_stats = None
    try:
        # Ensure no active connection to db_path before copying
        logger.info(f"Attempting to backup database from {db_path} to {db_backup_full_path}. Ensure DB is not locked.")
        shutil.copy2(db_path, db_backup_full_path)
        
        # Get stats from the original DB (or the backup, but original is fine)
        conn_for_stats = get_connection(db_path) # Uses imported get_connection
        stats = get_knowledge_graph_stats(conn_for_stats, db_path) # Uses get_knowledge_graph_stats from this file
        
        with open(stats_full_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        if context_logger:
            context_logger.log_event(
                "Knowledge Graph Backup Created",
                {
                    "db_backup_path": db_backup_full_path, "stats_path": stats_full_path,
                    "entity_count": stats["entity_count"], "relation_count": stats["relation_count"],
                    "observation_count": stats["observation_count"]
                }
            )
        logger.info(f"Created knowledge graph backup at {db_backup_full_path}")
        return db_backup_full_path, stats_full_path
        
    except Exception as e:
        error_msg = f"Error creating knowledge graph backup: {str(e)}"
        logger.error(error_msg)
        if context_logger:
            context_logger.log_event("Knowledge Graph Backup Error", {"error": error_msg})
        raise KnowledgeGraphError(error_msg) from e
    finally:
        if conn_for_stats:
            conn_for_stats.close()

def restore_knowledge_graph(
    # Similar to backup, connection management should be handled by the service layer.
    db_path: str,
    backup_file_path: str, 
    context_logger=None
) -> bool:
    """
    Restore the knowledge graph from a backup.
    The caller is responsible for managing database connections before/after restore.
    """
    if not os.path.exists(backup_file_path):
        raise ValueError(f"Backup file not found: {backup_file_path}")
    
    current_db_backup_path = db_path + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn_for_stats = None
    
    try:
        logger.info(f"Attempting to restore database {db_path} from {backup_file_path}. Ensure DB is not locked.")
        if os.path.exists(db_path):
            shutil.copy2(db_path, current_db_backup_path)
            logger.info(f"Backed up current database to {current_db_backup_path}")
        
        shutil.copy2(backup_file_path, db_path)
        
        conn_for_stats = get_connection(db_path)
        stats = get_knowledge_graph_stats(conn_for_stats, db_path)
        
        if context_logger:
            context_logger.log_event(
                "Knowledge Graph Restored",
                {
                    "backup_path": backup_file_path, "entity_count": stats["entity_count"],
                    "relation_count": stats["relation_count"], "observation_count": stats["observation_count"]
                }
            )
        logger.info(f"Restored knowledge graph from {backup_file_path}")
        return True
        
    except Exception as e:
        error_msg = f"Error restoring knowledge graph: {str(e)}"
        logger.error(error_msg)
        if os.path.exists(current_db_backup_path): # Try to restore the .bak if main restore failed
            try:
                shutil.copy2(current_db_backup_path, db_path)
                logger.info(f"Restored original database from {current_db_backup_path} after failed restore.")
            except Exception as restore_error:
                logger.error(f"CRITICAL: Failed to restore original database from {current_db_backup_path}: {restore_error}")
        
        if context_logger:
            context_logger.log_event("Knowledge Graph Restore Error", {"backup_path": backup_file_path, "error": error_msg})
        raise KnowledgeGraphError(error_msg) from e
    finally:
        if conn_for_stats:
            conn_for_stats.close()