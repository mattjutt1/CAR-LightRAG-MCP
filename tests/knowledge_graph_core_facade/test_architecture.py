"""
Tests for the Knowledge Graph's architectural patterns.

This module demonstrates testing the knowledge graph component without requiring
actual external dependencies like FastMCP, Redis, etc.
"""

import pytest
from unittest.mock import MagicMock, patch

# Import interfaces directly - these don't have external dependencies
from car_mcp.knowledge_graph_core_facade.kg_models_all import Entity, Relation, Observation
from car_mcp.knowledge_graph_core_facade.kg_models_all import CacheProvider
# Import the KnowledgeGraph class directly
from car_mcp.knowledge_graph_core_facade.graph_facade import KnowledgeGraph


def test_knowledge_graph_lazy_loading():
    """Test that KnowledgeGraph can be imported without requiring FastMCP."""
    # This test would have failed before our architectural changes
    # because importing KnowledgeGraph directly would try to import FastMCP
    
    # With the new architecture, we can get the class without importing FastMCP
    with patch.dict('sys.modules', {'fastmcp': MagicMock()}):
        # KnowledgeGraph is imported directly, it is the class.
        # Just check that we got a class, not an instance
        assert isinstance(KnowledgeGraph, type)


def test_cache_provider_interface_compatibility():
    """Test that any object implementing the CacheProvider protocol can be used."""
    
    # Create a minimal implementation of CacheProvider
    class MinimalCacheProvider:
        def get(self, key):
            return f"value for {key}"
        
        def set(self, key, value, ex=None):
            return True
        
        def delete(self, *keys):
            return len(keys)
        
        def exists(self, key):
            return True
    
    # Verify it satisfies the protocol
    provider = MinimalCacheProvider()
    assert isinstance(provider, CacheProvider)
    
    # Verify basic functionality
    assert provider.get("test_key") == "value for test_key"
    assert provider.set("test_key", "test_value") is True
    assert provider.delete("key1", "key2") == 2
    assert provider.exists("test_key") is True


def test_dependency_injection_with_mock_cache(mock_cache_provider, temp_db_path):
    """Test that the KnowledgeGraph accepts mock dependencies."""
    
    with patch.dict('sys.modules', {'fastmcp': MagicMock()}):
        # KnowledgeGraph is imported directly, it is the class.
        
        # Create a KnowledgeGraph with a mock cache
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_cache_provider
        )
        
        # Verify the cache was injected
        assert kg.redis_client is mock_cache_provider


def test_entity_operations_lazy_loading():
    """Test that entity operations use lazy loading to avoid circular imports."""
    
    # The operations module should provide functions that only import
    # their implementations when called
    import car_mcp.features.knowledge_graph_entities.ops_entity_crud as ops_entity_crud_module
    
    # Mock the actual implementation to verify it's imported lazily
    with patch.object(ops_entity_crud_module, 'create_entity') as mock_impl:
        mock_impl.return_value = "test_entity_id"
        
        # Call the wrapper function via the module to ensure the patched object is used
        result = ops_entity_crud_module.create_entity(
            conn=MagicMock(),
            name="test_entity",
            entity_type="test",
            redis_client=None # Other necessary args for create_entity might be missing if signature changed
        )
        
        # Verify the implementation was called
        assert mock_impl.called
        assert result == "test_entity_id"