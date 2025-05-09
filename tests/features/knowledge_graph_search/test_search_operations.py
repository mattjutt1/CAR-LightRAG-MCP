"""
Unit tests for Knowledge Graph search operations.

Tests the search functionality for entities in the Knowledge Graph component.
"""

import pytest
from unittest.mock import patch, MagicMock

from car_mcp.core.exceptions import KnowledgeGraphError


class TestEntitySearch:
    """Tests for entity search operations."""
    
    def test_search_entities_by_name(self, populated_knowledge_graph):
        """Test searching for entities by name."""
        kg = populated_knowledge_graph["graph"]
        
        # Search for an entity by name
        results = kg.search_entities("TestFunction")
        
        # Should return a list of results
        assert isinstance(results, list)
        assert len(results) > 0
        
        # First result should be the exact match
        assert results[0]["name"] == "TestFunction"
        
        # Results should include similarity scores
        assert "similarity" in results[0]
    
    def test_search_entities_by_partial_name(self, populated_knowledge_graph):
        """Test searching for entities by partial name."""
        kg = populated_knowledge_graph["graph"]
        
        # Search for entities with a partial name
        results = kg.search_entities("Test")
        
        # Should return entities that contain "Test" in their name
        assert len(results) > 0
        for result in results:
            assert "Test" in result["name"]
    
    def test_search_entities_by_type(self, populated_knowledge_graph):
        """Test searching for entities filtered by type."""
        kg = populated_knowledge_graph["graph"]
        
        # Search for entities of a specific type
        entity_type = "function"
        results = kg.search_entities("", entity_type=entity_type)
        
        # Should return only entities of the specified type
        assert len(results) > 0
        for result in results:
            assert result["entity_type"] == entity_type
    
    def test_search_entities_with_limit(self, populated_knowledge_graph):
        """Test searching for entities with a result limit."""
        kg = populated_knowledge_graph["graph"]
        
        # Create additional entities to ensure we have more than the limit
        for i in range(5):
            kg.create_entity(
                name=f"LimitTestEntity{i}",
                entity_type="test"
            )
        
        limit = 3
        results = kg.search_entities("LimitTest", limit=limit)
        
        # Should respect the limit
        assert len(results) <= limit
    
    def test_search_entities_with_min_similarity(self, populated_knowledge_graph):
        """Test searching for entities with a minimum similarity threshold."""
        kg = populated_knowledge_graph["graph"]
        
        # Set a high similarity threshold
        min_similarity = 0.9
        
        # Search with the high threshold
        high_threshold_results = kg.search_entities(
            "TestFunction",
            min_similarity=min_similarity
        )
        
        # Search with a lower threshold
        low_threshold_results = kg.search_entities(
            "TestFunction",
            min_similarity=0.1
        )
        
        # Higher threshold should return fewer results
        assert len(high_threshold_results) <= len(low_threshold_results)
        
        # Results from high threshold search should all meet the minimum similarity
        for result in high_threshold_results:
            assert result["similarity"] >= min_similarity
    
    def test_search_entities_semantic(self, populated_knowledge_graph, mock_embedding_function):
        """Test semantic search for entities using embeddings."""
        kg = populated_knowledge_graph["graph"]
        
        # Mock the embedding function to return a specific embedding for the query
        query_embedding = [0.5, 0.5, 0.5, 0.5, 0.5]
        
        with patch.object(kg, 'embedding_function', return_value=query_embedding):
            with patch('car_mcp.features.knowledge_graph_search.search_ops.semantic_search') as mock_semantic_search: # Updated path
                # Mock semantic search to return some results
                mock_results = [
                    {"id": "entity1", "name": "Entity1", "similarity": 0.95},
                    {"id": "entity2", "name": "Entity2", "similarity": 0.85}
                ]
                mock_semantic_search.return_value = mock_results
                
                # Perform the search
                results = kg.search_entities("semantic search query")
                
                # Semantic search should have been called with the query embedding
                mock_semantic_search.assert_called_once()
                
                # Results should match what the semantic search returned
                assert results == mock_results
    
    def test_search_entities_no_results(self, populated_knowledge_graph):
        """Test searching for entities with no matching results."""
        kg = populated_knowledge_graph["graph"]
        
        # Search for a non-existent entity
        results = kg.search_entities("NonExistentEntityName12345")
        
        # Should return an empty list, not None
        assert results == []
    
    def test_search_entities_empty_query(self, populated_knowledge_graph):
        """Test searching with an empty query string."""
        kg = populated_knowledge_graph["graph"]
        
        # Empty query should return entities based on other criteria or a default set
        results = kg.search_entities("")
        
        # Should return some results (implementation may vary)
        assert isinstance(results, list)
    
    def test_search_entities_cache_hit(self, populated_knowledge_graph, mock_redis_client):
        """Test entity search with a cache hit."""
        kg = populated_knowledge_graph["graph"]
        
        # Get search results first to set up expected result
        actual_results = kg.search_entities("TestFunction")
        
        # Mock Redis cache to simulate a cache hit
        cache_key = f"search:TestFunction:None:10:0.0"  # This should match the key format used in implementation
        import json
        mock_redis_client.get.return_value = json.dumps(actual_results)
        
        # Patch the database query to detect if it's called
        with patch('car_mcp.features.knowledge_graph_search.search_ops.search_entities_in_db') as mock_db_search: # Updated path
            # Search again, which should now use cache
            cached_results = kg.search_entities("TestFunction")
            
            # Database query should not be called
            mock_db_search.assert_not_called()
            
            # Should get the same results
            assert cached_results == actual_results
    
    def test_search_entities_db_error(self, populated_knowledge_graph):
        """Test handling database errors during entity search."""
        kg = populated_knowledge_graph["graph"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_search.search_ops.search_entities_in_db',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.search_entities("test")