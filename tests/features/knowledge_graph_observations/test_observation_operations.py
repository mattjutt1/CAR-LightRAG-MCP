"""
Unit tests for Knowledge Graph observation operations.

Tests the add, get, and delete operations for entity observations.
"""

import pytest
from unittest.mock import patch, MagicMock

from car_mcp.knowledge_graph_core_facade.kg_models_all import Observation
from car_mcp.core.exceptions import EntityNotFoundError, KnowledgeGraphError


class TestObservationAdd:
    """Tests for observation addition operations."""
    
    def test_add_observation_basic(self, populated_knowledge_graph):
        """Test adding a basic observation to an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        observation_text = "This is a test observation about the entity."
        
        observation_id = kg.add_observation(
            entity_id=entity_id,
            observation=observation_text
        )
        
        # Verify observation was added
        assert observation_id is not None
        assert isinstance(observation_id, str)
        
        # Verify the observation exists for the entity
        observations = kg.get_observations(entity_id)
        observation_found = False
        for obs in observations:
            if obs.id == observation_id and obs.observation == observation_text:
                observation_found = True
                break
        assert observation_found
    
    def test_add_observation_with_embedding(self, populated_knowledge_graph):
        """Test adding an observation with a custom embedding."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        
        test_embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
        observation_text = "This observation has a custom embedding."
        
        observation_id = kg.add_observation(
            entity_id=entity_id,
            observation=observation_text,
            embedding=test_embedding
        )
        
        # Verify the observation has the embedding
        observations = kg.get_observations(entity_id)
        for obs in observations:
            if obs.id == observation_id:
                assert obs.embedding == test_embedding
                break
    
    def test_add_observation_with_properties(self, populated_knowledge_graph):
        """Test adding an observation with custom properties."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity3_id"]
        
        properties = {
            "source": "documentation",
            "confidence": 0.9,
            "timestamp": "2023-01-01T12:00:00Z"
        }
        
        observation_text = "This observation has custom properties."
        
        observation_id = kg.add_observation(
            entity_id=entity_id,
            observation=observation_text,
            properties=properties
        )
        
        # Verify the observation has the properties
        observations = kg.get_observations(entity_id)
        for obs in observations:
            if obs.id == observation_id:
                assert obs.properties == properties
                break
    
    def test_add_observation_with_auto_embedding(self, populated_knowledge_graph):
        """Test adding an observation with automatic embedding generation."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        test_embedding = [0.9, 0.8, 0.7, 0.6, 0.5]
        
        # Mock the embedding function
        with patch.object(kg, 'embedding_function', return_value=test_embedding):
            observation_id = kg.add_observation(
                entity_id=entity_id,
                observation="This observation should have an auto-generated embedding."
            )
            
            # Verify the observation has the auto-generated embedding
            observations = kg.get_observations(entity_id)
            for obs in observations:
                if obs.id == observation_id:
                    assert obs.embedding == test_embedding
                    break
    
    def test_add_observation_nonexistent_entity(self, knowledge_graph):
        """Test adding an observation to a non-existent entity."""
        with pytest.raises(EntityNotFoundError):
            knowledge_graph.add_observation(
                entity_id="nonexistent-id",
                observation="This should fail because the entity doesn't exist."
            )
    
    def test_add_empty_observation(self, populated_knowledge_graph):
        """Test adding an empty observation to an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Empty observation should raise ValueError
        with pytest.raises(ValueError):
            kg.add_observation(
                entity_id=entity_id,
                observation=""
            )
        
        # Whitespace-only observation should also raise ValueError
        with pytest.raises(ValueError):
            kg.add_observation(
                entity_id=entity_id,
                observation="   "
            )
    
    def test_add_observation_db_error(self, populated_knowledge_graph):
        """Test handling database errors during observation addition."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_observations.ops_observation_crud.add_observation_to_db',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.add_observation(
                    entity_id=entity_id,
                    observation="This should fail due to database error."
                )


class TestObservationGet:
    """Tests for observation retrieval operations."""
    
    def test_get_observations(self, populated_knowledge_graph):
        """Test retrieving observations for an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        observations = kg.get_observations(entity_id)
        
        # Should return a list of Observation objects
        assert isinstance(observations, list)
        assert len(observations) > 0
        assert all(isinstance(obs, Observation) for obs in observations)
        
        # All observations should be for the specified entity
        assert all(obs.entity_id == entity_id for obs in observations)
    
    def test_get_observations_with_limit(self, populated_knowledge_graph):
        """Test retrieving a limited number of observations."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Add several observations to ensure we have more than the limit
        for i in range(5):
            kg.add_observation(
                entity_id=entity_id,
                observation=f"Test observation {i} for limit testing."
            )
        
        limit = 3
        observations = kg.get_observations(entity_id, limit=limit)
        
        # Should respect the limit
        assert len(observations) <= limit
    
    def test_get_observations_nonexistent_entity(self, knowledge_graph):
        """Test retrieving observations for a non-existent entity."""
        with pytest.raises(EntityNotFoundError):
            knowledge_graph.get_observations("nonexistent-id")
    
    def test_get_observations_empty(self, knowledge_graph, sample_entity_data):
        """Test retrieving observations for an entity with no observations."""
        # Create a new entity with no observations
        entity_id = knowledge_graph.create_entity(
            name=sample_entity_data["name"],
            entity_type=sample_entity_data["entity_type"]
        )
        
        observations = knowledge_graph.get_observations(entity_id)
        
        # Should return an empty list, not None
        assert observations == []
    
    def test_get_observations_cache_hit(self, populated_knowledge_graph, mock_redis_client):
        """Test retrieving observations with a cache hit."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Get observations first to set up expected result
        actual_observations = kg.get_observations(entity_id)
        
        # Convert observations to dictionary format for caching
        observation_dicts = [obs.to_dict() for obs in actual_observations]
        
        # Mock Redis cache to simulate a cache hit
        cache_key = f"observations:{entity_id}:100"  # This should match the key format used in implementation
        import json
        mock_redis_client.get.return_value = json.dumps(observation_dicts)
        
        # Patch the database query to detect if it's called
        with patch('car_mcp.features.knowledge_graph_observations.ops_observation_crud.get_observations_from_db') as mock_db_get: # Updated path
            # Get observations again, which should now use cache
            cached_observations = kg.get_observations(entity_id)
            
            # Database query should not be called
            mock_db_get.assert_not_called()
            
            # Should get the same number of observations
            assert len(cached_observations) == len(actual_observations)
    
    def test_get_observations_db_error(self, populated_knowledge_graph):
        """Test handling database errors during observation retrieval."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_observations.ops_observation_crud.get_observations_from_db',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.get_observations(entity_id)


class TestObservationDelete:
    """Tests for observation deletion operations."""
    
    def test_delete_observation(self, populated_knowledge_graph):
        """Test deleting an observation by ID."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        observation_id = populated_knowledge_graph["observations"]["observation1_id"]
        
        # Get the count of observations before deletion
        observations_before = kg.get_observations(entity_id)
        count_before = len(observations_before)
        
        # Delete the observation
        result = kg.delete_observation(observation_id)
        
        # Should return True for successful deletion
        assert result is True
        
        # Get observations after deletion
        observations_after = kg.get_observations(entity_id)
        count_after = len(observations_after)
        
        # Should have one less observation
        assert count_after == count_before - 1
        
        # The deleted observation should not be in the list
        observation_ids_after = [obs.id for obs in observations_after]
        assert observation_id not in observation_ids_after
    
    def test_delete_nonexistent_observation(self, knowledge_graph):
        """Test deleting a non-existent observation."""
        result = knowledge_graph.delete_observation("nonexistent-id")
        
        # Should return False for non-existent observations
        assert result is False
    
    def test_delete_observation_db_error(self, populated_knowledge_graph):
        """Test handling database errors during observation deletion."""
        kg = populated_knowledge_graph["graph"]
        observation_id = populated_knowledge_graph["observations"]["observation1_id"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_observations.ops_observation_crud.delete_observation_from_db',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.delete_observation(observation_id)
    
    def test_delete_observation_cache_invalidation(self, populated_knowledge_graph, mock_redis_client):
        """Test that caches are invalidated when an observation is deleted."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        observation_id = populated_knowledge_graph["observations"]["observation1_id"]
        
        # Delete the observation
        kg.delete_observation(observation_id)
        
        # Verify cache is invalidated for the entity
        mock_redis_client.delete.assert_any_call(f"observations:{entity_id}:*")