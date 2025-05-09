"""
Integration tests for the Knowledge Graph component.

Tests the complete functionality of the Knowledge Graph, including
interactions between different operations and with external dependencies.
"""

import pytest
import os
import tempfile
from unittest.mock import patch

from car_mcp.knowledge_graph_core_facade.graph_facade import KnowledgeGraph
from car_mcp.knowledge_graph_core_facade.kg_models_all import Entity, Relation, Observation
from car_mcp.core.exceptions import EntityNotFoundError, KnowledgeGraphError


class TestKnowledgeGraphIntegration:
    """Tests for the Knowledge Graph as a complete component."""
    
    def test_full_lifecycle(self, temp_db_path, mock_redis_client, mock_embedding_function):
        """Test a complete lifecycle of Knowledge Graph operations."""
        # Create a knowledge graph
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client,
            embedding_function=mock_embedding_function
        )
        
        try:
            # 1. Create entities
            entity1_id = kg.create_entity(
                name="Function1",
                entity_type="function",
                properties={"language": "python", "complexity": "low"}
            )
            
            entity2_id = kg.create_entity(
                name="Class1",
                entity_type="class",
                properties={"language": "python", "methods_count": 5}
            )
            
            entity3_id = kg.create_entity(
                name="Module1",
                entity_type="module",
                properties={"language": "python", "path": "/path/to/module.py"}
            )
            
            # Verify entities were created
            entity1 = kg.get_entity(entity1_id)
            entity2 = kg.get_entity(entity2_id)
            entity3 = kg.get_entity(entity3_id)
            
            assert entity1 is not None
            assert entity2 is not None
            assert entity3 is not None
            
            # 2. Create relations
            relation1_id = kg.create_relation(
                from_entity_id=entity1_id,
                to_entity_id=entity2_id,
                relation_type="defined_in",
                confidence=0.95
            )
            
            relation2_id = kg.create_relation(
                from_entity_id=entity2_id,
                to_entity_id=entity3_id,
                relation_type="part_of",
                confidence=1.0
            )
            
            relation3_id = kg.create_relation(
                from_entity_id=entity3_id,
                to_entity_id=entity1_id,
                relation_type="contains",
                confidence=0.9
            )
            
            # Verify relations were created
            relations1 = kg.get_relations(entity1_id)
            relations2 = kg.get_relations(entity2_id)
            relations3 = kg.get_relations(entity3_id)
            
            assert len(relations1) > 0
            assert len(relations2) > 0
            assert len(relations3) > 0
            
            # 3. Add observations
            obs1_id = kg.add_observation(
                entity_id=entity1_id,
                observation="This function is a utility for data processing."
            )
            
            obs2_id = kg.add_observation(
                entity_id=entity2_id,
                observation="This class implements core business logic."
            )
            
            # Verify observations were added
            observations1 = kg.get_observations(entity1_id)
            observations2 = kg.get_observations(entity2_id)
            
            assert len(observations1) == 1
            assert len(observations2) == 1
            assert observations1[0].observation == "This function is a utility for data processing."
            
            # 4. Search for entities
            search_results = kg.search_entities("Function")
            assert len(search_results) > 0
            assert search_results[0]["name"] == "Function1"
            
            # 5. Update an entity
            kg.update_entity(
                entity_id=entity1_id,
                name="UpdatedFunction",
                properties={"language": "python", "complexity": "medium", "updated": True}
            )
            
            # Verify update
            updated_entity = kg.get_entity(entity1_id)
            assert updated_entity.name == "UpdatedFunction"
            assert updated_entity.properties["complexity"] == "medium"
            assert updated_entity.properties["updated"] is True
            
            # 6. Delete a relation
            kg.delete_relation(relation1_id)
            
            # Verify relation is gone
            relations1_after = kg.get_relations(entity1_id)
            relation_ids = [rel["id"] for rel in relations1_after]
            assert relation1_id not in relation_ids
            
            # 7. Delete an observation
            kg.delete_observation(obs1_id)
            
            # Verify observation is gone
            observations1_after = kg.get_observations(entity1_id)
            assert len(observations1_after) == 0
            
            # 8. Get statistics
            stats = kg.get_stats()
            assert stats["entity_count"] == 3
            assert stats["relation_count"] == 2  # One was deleted
            assert stats["observation_count"] == 1  # One was deleted
            
            # 9. Backup the database
            with tempfile.TemporaryDirectory() as backup_dir:
                db_backup_path, stats_path = kg.backup(backup_dir)
                
                # Verify backup files exist
                assert os.path.exists(db_backup_path)
                assert os.path.exists(stats_path)
                
                # 10. Clear the database
                kg.clear()
                
                # Verify everything is cleared
                assert kg.get_stats()["entity_count"] == 0
                
                # 11. Restore from backup
                kg.restore(db_backup_path)
                
                # Verify restoration worked
                assert kg.get_stats()["entity_count"] == 3
                assert kg.get_entity(entity1_id) is not None
        
        finally:
            # Clean up
            kg.close()
    
    def test_error_propagation(self, temp_db_path):
        """Test that errors are properly propagated from low-level operations."""
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create an entity for testing
            entity_id = kg.create_entity(
                name="TestEntity",
                entity_type="test"
            )
            
            # Test error from entity operations
            # Patch where delete_entity is looked up by EntityManager
            with patch('car_mcp.features.knowledge_graph_entities.entity_manager.delete_entity',
                      side_effect=KnowledgeGraphError("Simulated DB error in entity_manager.delete_entity")):
                with pytest.raises(KnowledgeGraphError):
                    kg.delete_entity(entity_id)
            
            # Test error from relation operations with non-existent entity
            with pytest.raises(EntityNotFoundError):
                kg.create_relation(
                    from_entity_id=entity_id,
                    to_entity_id="nonexistent-id",
                    relation_type="test"
                )
            
            # Test error from observation operations with invalid input
            with pytest.raises(ValueError):
                kg.add_observation(
                    entity_id=entity_id,
                    observation=""  # Empty observation
                )
            
            # Test error from search operations
            # SearchManager imports 'search_entities' from '.search_ops'
            with patch('car_mcp.features.knowledge_graph_search.search_manager.search_entities',
                      side_effect=KnowledgeGraphError("Simulated DB error in search_manager.search_entities")):
                with pytest.raises(KnowledgeGraphError):
                    kg.search_entities("test")
            
            # Test error from maintenance operations
            # Patch where backup_knowledge_graph is looked up by MaintenanceManager
            with patch('car_mcp.features.knowledge_graph_maintenance.maintenance_manager.backup_knowledge_graph',
                      side_effect=KnowledgeGraphError("Simulated error in maintenance_manager.backup_knowledge_graph")):
                with pytest.raises(KnowledgeGraphError):
                    kg.backup("/tmp")
        
        finally:
            # Clean up
            kg.close()
    
    def test_concurrent_operations(self, temp_db_path, mock_redis_client):
        """Test the thread safety of Knowledge Graph operations."""
        # This test is a simplified simulation of concurrent operations
        # In a real-world scenario, you might use multiple threads or processes
        
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client
        )
        
        try:
            # Create some initial entities
            entity_ids = []
            for i in range(5):
                entity_id = kg.create_entity(
                    name=f"Entity{i}",
                    entity_type="test"
                )
                entity_ids.append(entity_id)
            
            # Simulate concurrent operations by interleaving different operation types
            for i in range(5):
                # Add a new entity
                new_entity_id = kg.create_entity(
                    name=f"NewEntity{i}",
                    entity_type="concurrent_test"
                )
                
                # Update an existing entity
                kg.update_entity(
                    entity_id=entity_ids[i],
                    name=f"UpdatedEntity{i}"
                )
                
                # Create relations between entities
                if i > 0:
                    kg.create_relation(
                        from_entity_id=entity_ids[i],
                        to_entity_id=entity_ids[i-1],
                        relation_type="connected_to"
                    )
                
                # Add observations
                kg.add_observation(
                    entity_id=entity_ids[i],
                    observation=f"Observation {i} on entity {i}"
                )
                
                # Search for entities
                search_results = kg.search_entities(f"Entity{i}")
                assert len(search_results) > 0
            
            # Verify final state
            stats = kg.get_stats()
            assert stats["entity_count"] == 10  # 5 initial + 5 new
            assert stats["relation_count"] == 4  # One for each iteration except first
            assert stats["observation_count"] == 5  # One for each initial entity
            
            # Verify updates were applied
            for i in range(5):
                entity = kg.get_entity(entity_ids[i])
                assert entity.name == f"UpdatedEntity{i}"
        
        finally:
            # Clean up
            kg.close()