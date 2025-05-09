"""
Test fixtures for knowledge graph relation operations.

This module imports and re-exports fixtures from the knowledge_graph_core_facade
test fixtures to make them available to the relation operation tests.
"""

import pytest
from car_mcp.tests.knowledge_graph_core_facade.conftest import (
    # Basic fixtures
    mock_cache_provider,
    mock_context_logger,
    mock_embedding_function,
    mock_redis_client,
    enhanced_mock_redis_client,
    deterministic_embedding_function,
    
    # Database fixtures
    temp_db_path,
    db_connection,
    in_memory_db_connection,
    
    # Knowledge graph fixtures
    knowledge_graph,
    knowledge_graph_class,
    in_memory_knowledge_graph,
    
    # Data fixtures
    sample_entity_data,
    sample_relation_data,
    sample_observation_data,
    
    # Populated fixtures
    populated_knowledge_graph,
    populated_in_memory_knowledge_graph,
    
    # Other fixtures
    backup_dir
)

# Re-export all imported fixtures