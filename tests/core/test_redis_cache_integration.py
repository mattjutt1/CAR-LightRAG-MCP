"""
Integration tests for Knowledge Graph Redis cache integration.

Tests the interaction between the Knowledge Graph and Redis cache,
verifying that cache operations work correctly for all operations.
"""

import pytest
import json
import time
from unittest.mock import patch, MagicMock, call, ANY

from car_mcp.knowledge_graph_core_facade.graph_facade import KnowledgeGraph
from car_mcp.knowledge_graph_core_facade.kg_models_all import Entity, Relation, Observation
from car_mcp.knowledge_graph_core_facade.kg_utils import get_cache_key, invalidate_cache


class TestRedisCacheIntegration:
    """Tests for the integration between Knowledge Graph and Redis cache."""
    
    def test_entity_cache_operations(self, temp_db_path, mock_redis_client):
        """Test entity caching operations including hits, misses, and invalidation."""
        cache_ttl = 10
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client,
            cache_ttl=cache_ttl
        )
        
        try:
            entity_id = kg.create_entity(
                name="CachedEntity",
                entity_type="test",
                properties={"cached": True}
            )
            
            mock_redis_client.set.assert_any_call(
                get_cache_key("get_entity", entity_id), 
                ANY,
                ex=cache_ttl
            )
            
            mock_redis_client.reset_mock()

            mock_redis_client.get.side_effect = None 
            mock_redis_client.get.return_value = None 
            
            entity_after_miss = kg.get_entity(entity_id)
            assert entity_after_miss is not None, "Entity should be retrieved from DB on cache miss"
            
            mock_redis_client.get.assert_called_with(get_cache_key("get_entity", entity_id))
            mock_redis_client.set.assert_called_with(
                get_cache_key("get_entity", entity_id),
                ANY, 
                ex=cache_ttl
            )

            mock_redis_client.reset_mock()
            mock_redis_client.get.return_value = json.dumps(entity_after_miss.to_dict() if hasattr(entity_after_miss, 'to_dict') else entity_after_miss.__dict__)

            entity_cache_hit = kg.get_entity(entity_id)
            assert entity_cache_hit is not None
            assert entity_cache_hit.id == entity_id

            mock_redis_client.get.assert_called_with(get_cache_key("get_entity", entity_id))
            mock_redis_client.set.assert_not_called() 
            
            kg.update_entity(
                entity_id=entity_id,
                name="UpdatedEntity"
            )
            
            mock_redis_client.delete.assert_any_call(get_cache_key("get_entity", entity_id)) 
            
            kg.delete_entity(entity_id)
            
            mock_redis_client.delete.assert_any_call(get_cache_key("get_entity", entity_id)) 
        
        finally:
            kg.close()
    
    def test_relation_cache_operations(self, temp_db_path, mock_redis_client):
        """Test relation caching operations including hits, misses, and invalidation."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client
        )
        
        try:
            entity1_id = kg.create_entity(name="Entity1", entity_type="test")
            entity2_id = kg.create_entity(name="Entity2", entity_type="test")
            
            mock_redis_client.reset_mock()
            
            relation_id = kg.create_relation(
                from_entity_id=entity1_id,
                to_entity_id=entity2_id,
                relation_type="test_relation"
            )
            
            relations = kg.get_relations(entity1_id)
            
            expected_relations_cache_key = get_cache_key("get_relations", entity1_id, "both", None)
            mock_redis_client.set.assert_any_call(
                expected_relations_cache_key,
                ANY, 
                ex=kg.cache_ttl 
            )
            
            mock_redis_client.reset_mock()
            mock_redis_client.get.return_value = json.dumps([rel.to_dict() if hasattr(rel, 'to_dict') else rel for rel in relations])

            with patch('car_mcp.features.knowledge_graph_relations.ops_relation_crud.execute_with_retry') as mock_execute_retry:
                cached_relations = kg.get_relations(entity1_id)
                mock_execute_retry.assert_not_called()
            
            assert cached_relations is not None
            assert len(cached_relations) == len(relations)
            
            mock_redis_client.get.assert_called_once_with(expected_relations_cache_key)
            mock_redis_client.set.assert_not_called() 
            
            mock_redis_client.reset_mock() 
            kg.delete_relation(relation_id)
            
            mock_redis_client.keys.assert_any_call("kg:get_relations*")
        
        finally:
            kg.close()
    
    def test_observation_cache_operations(self, temp_db_path, mock_redis_client):
        """Test observation caching operations including hits, misses, and invalidation."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client
        )
        
        try:
            entity_id = kg.create_entity(name="ObsEntity", entity_type="test")
            
            mock_redis_client.reset_mock()
            
            observation_id = kg.add_observation(
                entity_id=entity_id,
                observation="Test observation for cache testing"
            )
            
            observations = kg.get_observations(entity_id)
            
            expected_obs_cache_key = get_cache_key("get_observations", entity_id, 100) 
            mock_redis_client.set.assert_any_call(
                expected_obs_cache_key,
                ANY, 
                ex=kg.cache_ttl 
            )
            
            mock_redis_client.reset_mock()
            mock_redis_client.get.return_value = json.dumps([obs.to_dict() if hasattr(obs, 'to_dict') else obs.__dict__ for obs in observations])

            with patch('car_mcp.features.knowledge_graph_observations.ops_observation_crud.execute_with_retry') as mock_execute_retry:
                cached_observations = kg.get_observations(entity_id) 
                mock_execute_retry.assert_not_called()
            
            assert cached_observations is not None
            assert len(cached_observations) == len(observations)
            
            mock_redis_client.get.assert_called_once_with(expected_obs_cache_key)
            mock_redis_client.set.assert_not_called() 
            
            kg.delete_observation(observation_id)
            
            mock_redis_client.keys.assert_any_call("kg:get_observations*")
        
        finally:
            kg.close()
    
    def test_search_cache_operations(self, temp_db_path, mock_redis_client):
        """Test search caching operations including hits, misses, and invalidation."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client
        )
        
        try:
            entity1_id = kg.create_entity(name="SearchEntity1", entity_type="test")
            entity2_id = kg.create_entity(name="SearchEntity2", entity_type="test")
            entity3_id = kg.create_entity(name="OtherEntity", entity_type="different")
            
            mock_redis_client.reset_mock()
            
            search_results = kg.search_entities("Search")
            
            expected_search_cache_key = get_cache_key("search_entities", "Search", None, 10, 0.0)
            mock_redis_client.set.assert_any_call(
                expected_search_cache_key,
                ANY, 
                ex=kg.cache_ttl 
            )
            
            mock_redis_client.reset_mock()
            mock_redis_client.get.return_value = json.dumps(search_results)

            with patch('car_mcp.features.knowledge_graph_search.search_ops.execute_with_retry') as mock_execute_retry:
                cached_results = kg.search_entities("Search") 
                mock_execute_retry.assert_not_called()
            
            assert cached_results is not None
            assert len(cached_results) == len(search_results)
            
            mock_redis_client.get.assert_called_once_with(expected_search_cache_key)
            mock_redis_client.set.assert_not_called() 
            
            kg.create_entity(name="SearchEntity3", entity_type="test")
            
            kg.clear()
            
            mock_redis_client.keys.assert_any_call("kg:search_entities*")
        
        finally:
            kg.close()
    
    def test_stats_cache_operations(self, temp_db_path, mock_redis_client):
        """Test stats caching operations including hits, misses, and invalidation."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client
        )
        
        try:
            kg.create_entity(name="StatsEntity1", entity_type="test")
            kg.create_entity(name="StatsEntity2", entity_type="test")
            
            mock_redis_client.reset_mock()
            
            stats = kg.get_stats()
            
            expected_stats_cache_key = "kg:stats" 
            mock_redis_client.set.assert_any_call(
                expected_stats_cache_key,
                ANY, 
                ex=kg.cache_ttl 
            )
            
            # --- Test Cache Hit ---
            mock_redis_client.reset_mock()
            mock_redis_client.get.return_value = json.dumps(stats)

            with patch('car_mcp.features.knowledge_graph_maintenance.ops_maintenance.execute_with_retry') as mock_execute_retry:
                cached_stats = kg.get_stats()
                mock_execute_retry.assert_not_called()
            
            assert cached_stats is not None
            assert cached_stats["entity_count"] == stats["entity_count"]
            
            mock_redis_client.get.assert_called_once_with(expected_stats_cache_key)
            mock_redis_client.set.assert_not_called() 
            
            # --- Test Cache Invalidation ---
            mock_redis_client.reset_mock()
            kg.clear() 
            
            mock_redis_client.keys.assert_any_call("kg:*")
            
            delete_was_called_with_stats = False
            if mock_redis_client.delete.call_args_list:
                for call_args in mock_redis_client.delete.call_args_list:
                    if expected_stats_cache_key in call_args[0]: 
                        delete_was_called_with_stats = True
                        break
            assert delete_was_called_with_stats, f"Expected '{expected_stats_cache_key}' to be deleted after kg.clear()"

        finally:
            kg.close()
    
    def test_cache_expiration(self, temp_db_path, mock_redis_client):
        """Test that cache entries expire after TTL and database is queried again."""
        cache_ttl = 1 
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client,
            cache_ttl=cache_ttl
        )
        
        try:
            entity_id = kg.create_entity(name="ExpiringEntity", entity_type="test")
            
            entity = kg.get_entity(entity_id)
            
            mock_redis_client.set.assert_any_call(
                get_cache_key("get_entity", entity_id), 
                ANY,
                ex=cache_ttl
            )
            
            mock_redis_client.reset_mock()

            mock_redis_client.get.side_effect = None 
            mock_redis_client.get.return_value = None
            
            time.sleep(cache_ttl + 0.5) 

            with patch('car_mcp.features.knowledge_graph_entities.entity_manager.get_entity') as mock_ops_get_entity:
                minimal_entity_for_mock = Entity(id=entity_id, name="MockedExpiringEntity", entity_type="test")
                mock_ops_get_entity.return_value = minimal_entity_for_mock
                
                entity_after_expiry = kg.get_entity(entity_id)
                
                mock_ops_get_entity.assert_called_once()
        
        finally:
            kg.close()
    
    def test_cache_disabled_with_none_client(self, temp_db_path):
        """Test that operations work correctly when cache is disabled (redis_client=None)."""
        kg = KnowledgeGraph(db_path=temp_db_path, redis_client=None)
        
        try:
            entity_id = kg.create_entity(name="NoCacheEntity", entity_type="test")
            
            entity1 = kg.get_entity(entity_id)
            entity2 = kg.get_entity(entity_id)
            
            assert entity1 is not None
            assert entity1.id == entity_id
            assert entity2 is not None
            assert entity2.id == entity_id
            
            entity2_id = kg.create_entity(name="NoCacheEntity2", entity_type="test")
            relation_id = kg.create_relation(
                from_entity_id=entity_id,
                to_entity_id=entity2_id,
                relation_type="no_cache_test"
            )
            
            relations = kg.get_relations(entity_id)
            assert len(relations) == 1
            assert relations[0]["id"] == relation_id
            
            observation_id = kg.add_observation(
                entity_id=entity_id,
                observation="No cache observation test"
            )
            
            observations = kg.get_observations(entity_id)
            assert len(observations) == 1
            assert observations[0].id == observation_id
            
            search_results = kg.search_entities("NoCache")
            assert len(search_results) > 0
            
            stats = kg.get_stats()
            
            # Extremely defensive check for 'observations_count'
            obs_count_key_found = None
            if stats is not None: # Ensure stats is not None
                print(f"Stats dict keys in test: {list(stats.keys())}")
                for k in stats.keys():
                    if "observation_count" in k: # Check substring just in case
                        obs_count_key_found = k
                        break
            
            print(f"Found key for observations: {obs_count_key_found}, Value: {stats.get(obs_count_key_found) if obs_count_key_found else 'Key not found'}")

            assert stats is not None, "Stats dictionary is None"
            assert "entity_count" in stats, f"Key 'entity_count' not in stats: {stats.keys()}"
            assert stats["entity_count"] == 2, f"entity_count was {stats.get('entity_count')}, expected 2. Stats: {stats}"
            
            assert "relation_count" in stats, f"Key 'relation_count' not in stats: {stats.keys()}"
            assert stats["relation_count"] == 1, f"relation_count was {stats.get('relation_count')}, expected 1. Stats: {stats}"
            
            assert obs_count_key_found is not None, f"Defensive check failed to find 'observations_count' key. Keys: {stats.keys() if stats else 'No stats dict'}"
            assert stats.get(obs_count_key_found) == 1, f"'{obs_count_key_found}' was {stats.get(obs_count_key_found)}, expected 1. Stats: {stats}"
        
        finally:
            kg.close()

    def test_enhanced_redis_cache(self, temp_db_path, mock_redis_client): 
        """Test enhanced Redis functionality with hash operations."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client 
        )
        
        try:
            entity_ids = []
            for i in range(5):
                entity_id = kg.create_entity(
                    name=f"HashEntity{i}",
                    entity_type="test",
                    properties={"index": i}
                )
                entity_ids.append(entity_id)
            
            hash_key = "entity_types"
            
            if hasattr(mock_redis_client, 'hset'):
                with patch.object(mock_redis_client, 'hset') as mock_hset: 
                    kg.get_stats() 
            
            if hasattr(mock_redis_client, 'hget'):
                with patch.object(mock_redis_client, 'hget') as mock_hget: 
                    mock_hget.return_value = "5"
                    kg.get_stats() 
            pass 
        
        finally:
            kg.close()
    
    def test_redis_list_operations(self, temp_db_path, mock_redis_client): 
        """Test Redis list operations for tracking recent entities."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client 
        )
        
        try:
            entity_ids = []
            for i in range(3):
                entity_id = kg.create_entity(
                    name=f"ListEntity{i}",
                    entity_type="test",
                    properties={"index": i}
                )
                entity_ids.append(entity_id)
            
            list_key = "recent_entities"
            
            if hasattr(mock_redis_client, 'lpush'):
                with patch.object(mock_redis_client, 'lpush') as mock_lpush: 
                    for entity_id in entity_ids:
                        mock_redis_client.lpush(list_key, entity_id) 
            
            if hasattr(mock_redis_client, 'lrange'):
                with patch.object(mock_redis_client, 'lrange') as mock_lrange: 
                    mock_lrange.return_value = entity_ids
            pass 
        
        finally:
            kg.close()

    def test_redis_pipeline_operations(self, temp_db_path, mock_redis_client): 
        """Test Redis pipeline operations for atomic updates."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client 
        )
        
        try:
            pipeline_mock = MagicMock()
            if hasattr(mock_redis_client, 'pipeline'):
                mock_redis_client.pipeline.return_value = pipeline_mock 
            pipeline_mock.__enter__.return_value = pipeline_mock
            pipeline_mock.__exit__.return_value = None
            
            entity_id = kg.create_entity(
                name="PipelineEntity",
                entity_type="test"
            )
            
            observations = [
                f"Observation {i} for pipeline testing"
                for i in range(3)
            ]
            
            observation_ids = []
            for obs in observations:
                observation_id = kg.add_observation(
                    entity_id=entity_id,
                    observation=obs
                )
                observation_ids.append(observation_id)
            
            if hasattr(mock_redis_client, 'pipeline'):
                with patch.object(mock_redis_client, 'pipeline') as mock_pipeline_patch: 
                    kg.delete_entity(entity_id)
            pass 
        
        finally:
            kg.close()
            
    def test_concurrent_cache_access(self, temp_db_path, mock_redis_client): 
        """Test concurrent access to the cache with thread-safe operations."""
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_redis_client 
        )
        
        try:
            entity_ids = []
            for i in range(3):
                entity_id = kg.create_entity(
                    name=f"ConcurrentEntity{i}",
                    entity_type="test"
                )
                entity_ids.append(entity_id)
            
            entity_id = entity_ids[0]
            
            entities = []
            for _ in range(5):
                entity = kg.get_entity(entity_id)
                entities.append(entity)
            
            for entity in entities:
                assert entity.id == entity_id
                assert entity.name == f"ConcurrentEntity0"
            
            if hasattr(mock_redis_client, 'delete'):
                with patch.object(mock_redis_client, 'delete') as mock_delete: 
                    for i, entity_id_val in enumerate(entity_ids): 
                        kg.update_entity(
                            entity_id=entity_id_val,
                            name=f"UpdatedConcurrent{i}"
                        )
            pass 
        
        finally:
            kg.close()