"""
Unit tests for Knowledge Graph maintenance operations.

Tests the maintenance operations such as backup, restore, clear, and stats.
"""

import pytest
import os
import json
from unittest.mock import patch, MagicMock

from car_mcp.core.exceptions import KnowledgeGraphError


class TestMaintenanceClear:
    """Tests for clear operations that remove all data from the knowledge graph."""
    
    def test_clear_graph(self, populated_knowledge_graph):
        """Test clearing all data from the knowledge graph."""
        kg = populated_knowledge_graph["graph"]
        
        # Get initial counts
        initial_stats = kg.get_stats()
        assert initial_stats["entities_count"] > 0
        assert initial_stats["relations_count"] > 0
        assert initial_stats["observations_count"] > 0
        
        # Clear the graph
        result = kg.clear()
        
        # Result should contain deleted counts
        assert isinstance(result, dict)
        assert "entities" in result
        assert "relations" in result
        assert "observations" in result
        
        # Get stats after clearing
        final_stats = kg.get_stats()
        assert final_stats["entities_count"] == 0
        assert final_stats["relations_count"] == 0
        assert final_stats["observations_count"] == 0
        
        # Verify entities are gone by checking a previously existing entity
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        entity = kg.get_entity(entity_id)
        assert entity is None
    
    def test_clear_empty_graph(self, knowledge_graph):
        """Test clearing an already empty knowledge graph."""
        # Clear an empty graph
        result = knowledge_graph.clear()
        
        # Should succeed without errors
        assert isinstance(result, dict)
        assert result["entities"] == 0
        assert result["relations"] == 0
        assert result["observations"] == 0
    
    def test_clear_cache_invalidation(self, populated_knowledge_graph, mock_redis_client):
        """Test that caches are invalidated when the graph is cleared."""
        kg = populated_knowledge_graph["graph"]
        
        # Clear the graph
        kg.clear()
        
        # Verify the cache was flushed
        mock_redis_client.delete.assert_called_with("*")
    
    def test_clear_db_error(self, populated_knowledge_graph):
        """Test handling database errors during clear operation."""
        kg = populated_knowledge_graph["graph"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_maintenance.ops_maintenance.clear_all_data',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.clear()


class TestMaintenanceBackup:
    """Tests for backup operations that save the knowledge graph state."""
    
    def test_backup_graph(self, populated_knowledge_graph, backup_dir):
        """Test backing up the knowledge graph database."""
        kg = populated_knowledge_graph["graph"]
        
        # Perform backup
        db_path, stats_path = kg.backup(backup_dir)
        
        # Verify backup files were created
        assert os.path.exists(db_path)
        assert os.path.exists(stats_path)
        
        # Verify the stats file contains valid JSON with the right structure
        with open(stats_path, 'r') as f:
            stats = json.load(f)
            assert "timestamp" in stats
            assert "entities_count" in stats
            assert "relations_count" in stats
            assert "observations_count" in stats
            assert "database_size" in stats
    
    def test_backup_empty_graph(self, knowledge_graph, backup_dir):
        """Test backing up an empty knowledge graph."""
        # Backup an empty graph
        db_path, stats_path = knowledge_graph.backup(backup_dir)
        
        # Should succeed and create backup files
        assert os.path.exists(db_path)
        assert os.path.exists(stats_path)
        
        # Stats should show zero counts
        with open(stats_path, 'r') as f:
            stats = json.load(f)
            assert stats["entities_count"] == 0
            assert stats["relations_count"] == 0
            assert stats["observations_count"] == 0
    
    def test_backup_path_error(self, populated_knowledge_graph):
        """Test handling errors when the backup path is invalid."""
        kg = populated_knowledge_graph["graph"]
        
        # Try to backup to a non-existent path that we can't create
        with pytest.raises(KnowledgeGraphError):
            kg.backup("/path/that/cannot/exist")


class TestMaintenanceRestore:
    """Tests for restore operations that load the knowledge graph state from backup."""
    
    def test_restore_graph(self, knowledge_graph, populated_knowledge_graph, backup_dir):
        """Test restoring the knowledge graph database from a backup."""
        source_kg = populated_knowledge_graph["graph"]
        target_kg = knowledge_graph
        
        # Get original stats to compare after restore
        original_stats = source_kg.get_stats()
        
        # Create backup from the populated graph
        db_path, _ = source_kg.backup(backup_dir)
        
        # Restore to a different knowledge graph instance
        result = target_kg.restore(db_path)
        
        # Restore should succeed
        assert result is True
        
        # Verify the restored graph has the same stats as the original
        restored_stats = target_kg.get_stats()
        assert restored_stats["entities_count"] == original_stats["entities_count"]
        assert restored_stats["relations_count"] == original_stats["relations_count"]
        assert restored_stats["observations_count"] == original_stats["observations_count"]
        
        # Verify we can retrieve an entity from the original graph in the restored graph
        entity_id = populated_knowledge_graph["entities"]["entity1_id"]
        entity = target_kg.get_entity(entity_id)
        assert entity is not None
        assert entity.id == entity_id
    
    def test_restore_nonexistent_backup(self, knowledge_graph):
        """Test restoring from a non-existent backup file."""
        with pytest.raises(KnowledgeGraphError):
            knowledge_graph.restore("/nonexistent/backup.db")
    
    def test_restore_invalid_backup(self, knowledge_graph, backup_dir):
        """Test restoring from an invalid backup file."""
        # Create an invalid "backup" file
        invalid_backup_path = os.path.join(backup_dir, "invalid.db")
        with open(invalid_backup_path, 'w') as f:
            f.write("This is not a valid SQLite database file.")
        
        # Try to restore from the invalid file
        with pytest.raises(KnowledgeGraphError):
            knowledge_graph.restore(invalid_backup_path)
    
    def test_restore_cache_invalidation(self, knowledge_graph, populated_knowledge_graph, 
                                       backup_dir, mock_redis_client):
        """Test that caches are invalidated when restoring a graph."""
        source_kg = populated_knowledge_graph["graph"]
        
        # Create backup
        db_path, _ = source_kg.backup(backup_dir)
        
        # Restore to another graph
        knowledge_graph.restore(db_path)
        
        # Verify the cache was flushed
        mock_redis_client.delete.assert_called_with("*")


class TestMaintenanceStats:
    """Tests for statistics operations that provide information about the knowledge graph."""
    
    def test_get_stats(self, populated_knowledge_graph):
        """Test getting statistics about the knowledge graph."""
        kg = populated_knowledge_graph["graph"]
        
        stats = kg.get_stats()
        
        # Verify the stats have the expected structure
        assert isinstance(stats, dict)
        assert "timestamp" in stats
        assert "entities_count" in stats
        assert "relations_count" in stats
        assert "observations_count" in stats
        assert "database_size" in stats
        assert "entity_types" in stats
        assert "relation_types" in stats
        
        # Verify counts reflect the populated graph
        assert stats["entities_count"] > 0
        assert stats["relations_count"] > 0
        assert stats["observations_count"] > 0
    
    def test_get_stats_empty_graph(self, knowledge_graph):
        """Test getting statistics about an empty knowledge graph."""
        stats = knowledge_graph.get_stats()
        
        # Empty graph should have zero counts
        assert stats["entities_count"] == 0
        assert stats["relations_count"] == 0
        assert stats["observations_count"] == 0
        assert stats["entity_types"] == {}
        assert stats["relation_types"] == {}
        
    def test_get_stats_cache(self, populated_knowledge_graph, mock_redis_client):
        """Test caching of statistics."""
        kg = populated_knowledge_graph["graph"]
        
        # Get stats first to generate cache
        actual_stats = kg.get_stats()
        
        # Mock Redis cache to simulate a cache hit
        cache_key = "stats"  # This should match the key format used in implementation
        import json
        mock_redis_client.get.return_value = json.dumps(actual_stats)
        
        # Patch the database query to detect if it's called
        with patch('car_mcp.features.knowledge_graph_maintenance.ops_maintenance.get_stats_from_db') as mock_db_stats: # Updated path
            # Get stats again, which should now use cache
            cached_stats = kg.get_stats()
            
            # Database query should not be called
            mock_db_stats.assert_not_called()
            
            # Should get the same stats
            assert cached_stats == actual_stats
    
    def test_get_stats_db_error(self, populated_knowledge_graph):
        """Test handling database errors during stats retrieval."""
        kg = populated_knowledge_graph["graph"]
        
        # Mock the internal database operation to raise an exception
        with patch('car_mcp.features.knowledge_graph_maintenance.ops_maintenance.get_stats_from_db',  # Updated path
                  side_effect=Exception("Database error")):
            with pytest.raises(KnowledgeGraphError):
                kg.get_stats()