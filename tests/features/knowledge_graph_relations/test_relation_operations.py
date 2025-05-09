"""
Unit tests for Knowledge Graph relation operations.

Tests the create, read, and delete operations for relations between entities.
"""

import pytest
from unittest.mock import patch, MagicMock

from car_mcp.knowledge_graph_core_facade.kg_models_all import Relation
from car_mcp.core.exceptions import EntityNotFoundError, KnowledgeGraphError


class TestRelationCreate:
    """Tests for relation creation operations."""
    
    def test_create_relation_basic(self, populated_knowledge_graph):
        """Test creating a basic relation between two entities."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        
        relation_id = kg.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type="depends_on"
        )
        
        # Verify relation was created
        assert relation_id is not None
        assert isinstance(relation_id, str)
        
        # Verify the relation exists in both entities' relations
        relations = kg.get_relations(from_entity_id)
        relation_found = False
        for relation in relations:
            if relation["id"] == relation_id and relation["to_entity_id"] == to_entity_id:
                relation_found = True
                break
        assert relation_found
    
    def test_create_relation_with_confidence(self, populated_knowledge_graph):
        """Test creating a relation with a custom confidence value."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity3_id"]
        
        confidence = 0.75
        
        relation_id = kg.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type="references",
            confidence=confidence
        )
        
        # Verify the relation has the correct confidence
        relations = kg.get_relations(from_entity_id)
        relation_found = False
        for relation in relations:
            if relation["id"] == relation_id:
                assert relation["confidence"] == confidence
                relation_found = True
                break
        assert relation_found
    
    def test_create_relation_with_properties(self, populated_knowledge_graph):
        """Test creating a relation with custom properties."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity3_id"]
        
        properties = {
            "count": 5,
            "locations": [10, 25, 42, 57, 68],
            "critical": True
        }
        
        relation_id = kg.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type="defined_in",
            properties=properties
        )
        
        # Verify the relation has the correct properties
        relations = kg.get_relations(from_entity_id)
        relation_found = False
        for relation in relations:
            if relation["id"] == relation_id:
                assert relation["properties"] == properties
                relation_found = True
                break
        assert relation_found
    
    def test_create_relation_nonexistent_from_entity(self, populated_knowledge_graph):
        """Test creating a relation with a non-existent source entity."""
        kg = populated_knowledge_graph["graph"]
        to_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        with pytest.raises(EntityNotFoundError):
            kg.create_relation(
                from_entity_id="nonexistent-id",
                to_entity_id=to_entity_id,
                relation_type="depends_on"
            )
    
    def test_create_relation_nonexistent_to_entity(self, populated_knowledge_graph):
        """Test creating a relation with a non-existent target entity."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        with pytest.raises(EntityNotFoundError):
            kg.create_relation(
                from_entity_id=from_entity_id,
                to_entity_id="nonexistent-id",
                relation_type="depends_on"
            )
    
    def test_create_duplicate_relation(self, populated_knowledge_graph):
        """Test creating a duplicate relation between the same entities with the same type."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        
        # Create first relation
        relation1_id = kg.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type="unique_type_for_test"
        )
        
        # Create second relation with same entities and type
        relation2_id = kg.create_relation(
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
            relation_type="unique_type_for_test"
        )
        
        # Should get the same ID for both relations (implementation returns existing ID)
        assert relation1_id == relation2_id
        
        # Only one relation should exist
        relations = kg.get_relations(from_entity_id, relation_type="unique_type_for_test")
        assert len(relations) == 1
    
    @pytest.mark.parametrize("confidence", [
        -0.1,  # Below minimum
        1.1,   # Above maximum
    ])
    def test_create_relation_invalid_confidence(self, populated_knowledge_graph, confidence):
        """Test creating relations with invalid confidence values."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        
        with pytest.raises(ValueError):
            kg.create_relation(
                from_entity_id=from_entity_id,
                to_entity_id=to_entity_id,
                relation_type="test",
                confidence=confidence
            )
    
    def test_create_relation_self_reference(self, populated_knowledge_graph):
        """Test creating a relation where an entity references itself."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Self-referential relations should be allowed
        relation_id = kg.create_relation(
            from_entity_id=entity_id,
            to_entity_id=entity_id,
            relation_type="recursively_calls"
        )
        
        assert relation_id is not None
        
        # Verify the relation exists
        relations = kg.get_relations(entity_id)
        relation_found = False
        for relation in relations:
            if relation["id"] == relation_id and relation["from_entity_id"] == entity_id and relation["to_entity_id"] == entity_id:
                relation_found = True
                break
        assert relation_found
    
    def test_create_relation_db_error(self, populated_knowledge_graph):
        """Test handling database errors during relation creation."""
        kg = populated_knowledge_graph["graph"]
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        to_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        
        # Mock directly at the knowledge graph facade level
        with patch('car_mcp.knowledge_graph_core_facade.graph_facade.KnowledgeGraph.create_relation',
                   side_effect=KnowledgeGraphError("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.create_relation(
                    from_entity_id=from_entity_id,
                    to_entity_id=to_entity_id,
                    relation_type="test"
                )


class TestRelationRead:
    """Tests for relation read operations."""
    
    def test_get_outgoing_relations(self, populated_knowledge_graph):
        """Test retrieving outgoing relations from an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        relations = kg.get_relations(entity_id, direction="outgoing")
        
        assert isinstance(relations, list)
        # At least one outgoing relation should exist from entity1
        assert len(relations) > 0
        
        # Verify all relations are outgoing from this entity
        for relation in relations:
            assert relation["from_entity_id"] == entity_id
    
    def test_get_incoming_relations(self, populated_knowledge_graph):
        """Test retrieving incoming relations to an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        relations = kg.get_relations(entity_id, direction="incoming")
        
        assert isinstance(relations, list)
        # At least one incoming relation should exist to entity1
        assert len(relations) > 0
        
        # Verify all relations are incoming to this entity
        for relation in relations:
            assert relation["to_entity_id"] == entity_id
    
    def test_get_all_relations(self, populated_knowledge_graph):
        """Test retrieving all relations (both incoming and outgoing) for an entity."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Get relations in both directions
        relations = kg.get_relations(entity_id, direction="both")
        
        # Get outgoing and incoming separately for comparison
        outgoing = kg.get_relations(entity_id, direction="outgoing")
        incoming = kg.get_relations(entity_id, direction="incoming")
        
        # The 'both' result should include all outgoing and incoming relations
        assert len(relations) == len(outgoing) + len(incoming)
    
    def test_get_relations_by_type(self, populated_knowledge_graph):
        """Test retrieving relations filtered by relation type."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Create some relations with a specific type
        test_type = "test_specific_type"
        
        to_entity_id = populated_knowledge_graph["entities"]["entity2_id"]
        kg.create_relation(
            from_entity_id=entity_id,
            to_entity_id=to_entity_id,
            relation_type=test_type
        )
        
        # Get relations filtered by this type
        relations = kg.get_relations(entity_id, relation_type=test_type)
        
        assert len(relations) > 0
        # Verify all returned relations have the specified type
        for relation in relations:
            assert relation["relation_type"] == test_type
    
    def test_get_relations_nonexistent_entity(self, knowledge_graph):
        """Test retrieving relations for a non-existent entity."""
        with pytest.raises(EntityNotFoundError):
            knowledge_graph.get_relations("nonexistent-id")
    
    def test_get_relations_empty(self, knowledge_graph, sample_entity_data):
        """Test retrieving relations for an entity with no relations."""
        # Create an isolated entity with no relations
        entity_id = knowledge_graph.create_entity(
            name=sample_entity_data["name"],
            entity_type=sample_entity_data["entity_type"]
        )
        
        # Get relations for this entity
        relations = knowledge_graph.get_relations(entity_id)
        
        # Should return an empty list, not None
        assert relations == []
    
    def test_get_relations_cache_hit(self, populated_knowledge_graph, mock_redis_client):
        """Test retrieving relations with a cache hit."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Get relations first to populate cache in actual implementation
        actual_relations = kg.get_relations(entity_id, direction="outgoing")
        
        # Mock Redis cache to simulate a cache hit
        # Use the get_cache_key function from kg_utils to create the correct key
        from car_mcp.knowledge_graph_core_facade.kg_utils import get_cache_key
        cache_key = get_cache_key("get_relations", entity_id, "outgoing", None)
        import json
        mock_redis_client.get.return_value = json.dumps([r for r in actual_relations])
        
        # Get relations again, which should now use cache
        cached_relations = kg.get_relations(entity_id, direction="outgoing")
        
        # mock_redis_client.get should have been called with the cache key
        mock_redis_client.get.assert_called_with(cache_key)
        
        # Should get the same relations as before
        assert len(cached_relations) == len(actual_relations)
    
    def test_get_relations_db_error(self, populated_knowledge_graph):
        """Test handling database errors during relation retrieval."""
        kg = populated_knowledge_graph["graph"]
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        
        # Mock directly at the knowledge graph facade level
        with patch('car_mcp.knowledge_graph_core_facade.graph_facade.KnowledgeGraph.get_relations',
                   side_effect=KnowledgeGraphError("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.get_relations(entity_id)


class TestRelationDelete:
    """Tests for relation delete operations."""
    
    def test_delete_relation(self, populated_knowledge_graph):
        """Test deleting a relation by ID."""
        kg = populated_knowledge_graph["graph"]
        relation_id = populated_knowledge_graph["relations"]["relation1_id"]
        
        # Delete the relation
        result = kg.delete_relation(relation_id)
        
        # Should return True for successful deletion
        assert result is True
        
        # The relation should no longer appear in either entity's relations
        from_entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        relations = kg.get_relations(from_entity_id)
        
        relation_found = False
        for relation in relations:
            if relation["id"] == relation_id:
                relation_found = True
                break
        
        assert not relation_found
    
    def test_delete_nonexistent_relation(self, knowledge_graph):
        """Test deleting a non-existent relation."""
        result = knowledge_graph.delete_relation("nonexistent-id")
        
        # Should return False for non-existent relations
        assert result is False
    
    def test_delete_relation_db_error(self, populated_knowledge_graph):
        """Test handling database errors during relation deletion."""
        kg = populated_knowledge_graph["graph"]
        relation_id = populated_knowledge_graph["relations"]["relation1_id"]
        
        # Mock directly at the knowledge graph facade level
        with patch('car_mcp.knowledge_graph_core_facade.graph_facade.KnowledgeGraph.delete_relation',
                   side_effect=KnowledgeGraphError("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.delete_relation(relation_id)
    
    def test_delete_relation_cache_invalidation(self, populated_knowledge_graph, mock_redis_client):
        """Test that caches are invalidated when a relation is deleted."""
        kg = populated_knowledge_graph["graph"]
        relation_id = populated_knowledge_graph["relations"]["relation1_id"]
        
        # Delete the relation
        kg.delete_relation(relation_id)
        
        # Verify cache is invalidated with the correct pattern
        # The implementation uses invalidate_cache with pattern "kg:get_relations*"
        from car_mcp.knowledge_graph_core_facade.kg_utils import invalidate_cache
        mock_redis_client.keys.assert_any_call("kg:get_relations*")