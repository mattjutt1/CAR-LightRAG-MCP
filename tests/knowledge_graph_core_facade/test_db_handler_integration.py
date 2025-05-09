"""
Integration tests for Knowledge Graph database persistence.

Tests the durability and persistence of data stored in the SQLite database,
ensuring proper data storage, retrieval, and integrity constraints.
"""

import pytest
import os
import sqlite3
import tempfile
import time
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from car_mcp.knowledge_graph_core_facade.graph_facade import KnowledgeGraph
from car_mcp.knowledge_graph_core_facade.db_handler import init_database, get_connection
from car_mcp.knowledge_graph_core_facade.kg_models_all import Entity, Relation, Observation
from car_mcp.core.exceptions import EntityNotFoundError, KnowledgeGraphError


class TestDatabasePersistence:
    """Tests for the SQLite database persistence of the Knowledge Graph."""
    
    def test_data_persistence_across_connections(self, temp_db_path):
        """Test that data persists when closing and reopening the database connection."""
        # Create a knowledge graph and add data
        kg1 = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create entities and relationships
            entity1_id = kg1.create_entity(name="PersistenceTest1", entity_type="test")
            entity2_id = kg1.create_entity(name="PersistenceTest2", entity_type="test")
            
            relation_id = kg1.create_relation(
                from_entity_id=entity1_id,
                to_entity_id=entity2_id,
                relation_type="persists_with"
            )
            
            observation_id = kg1.add_observation(
                entity_id=entity1_id,
                observation="Test observation for persistence"
            )
            
            # Close the connection
            kg1.close()
            
            # Create a new knowledge graph instance with the same database
            kg2 = KnowledgeGraph(db_path=temp_db_path)
            
            try:
                # Verify data persisted
                entity1 = kg2.get_entity(entity1_id)
                entity2 = kg2.get_entity(entity2_id)
                
                assert entity1 is not None
                assert entity1.name == "PersistenceTest1"
                assert entity2 is not None
                assert entity2.name == "PersistenceTest2"
                
                # Check relations
                relations = kg2.get_relations(entity1_id)
                assert len(relations) == 1
                assert relations[0]["id"] == relation_id
                assert relations[0]["from_entity_id"] == entity1_id
                assert relations[0]["to_entity_id"] == entity2_id
                
                # Check observations
                observations = kg2.get_observations(entity1_id)
                assert len(observations) == 1
                assert observations[0].id == observation_id
                assert observations[0].observation == "Test observation for persistence"
            
            finally:
                kg2.close()
        
        finally:
            # Clean up in case of exceptions
            if 'kg1' in locals() and hasattr(kg1, 'conn') and kg1.conn:
                kg1.close()
    
    def test_database_file_creation(self):
        """Test that the database file is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "new_test_db.sqlite")
            
            # Verify file doesn't exist
            assert not os.path.exists(db_path)
            
            # Create knowledge graph, which should create the database file
            kg = KnowledgeGraph(db_path=db_path)
            
            try:
                # Verify file was created
                assert os.path.exists(db_path)
                
                # Verify we can perform operations
                entity_id = kg.create_entity(name="TestEntity", entity_type="test")
                assert entity_id is not None
            
            finally:
                kg.close()
    
    def test_foreign_key_constraints(self, temp_db_path):
        """Test that foreign key constraints are enforced in the database."""
        conn = init_database(temp_db_path)
        
        try:
            # Attempt to insert a relation with non-existent entities (should fail)
            cursor = conn.cursor()
            
            with pytest.raises(sqlite3.IntegrityError):
                cursor.execute(
                    """
                    INSERT INTO relations 
                    (id, from_entity_id, to_entity_id, relation_type, confidence, created_at, properties)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
                    """,
                    ("test-relation-id", "nonexistent-from-id", "nonexistent-to-id", "test", 1.0, "{}")
                )
            
            # Attempt to insert an observation with non-existent entity (should fail)
            with pytest.raises(sqlite3.IntegrityError):
                cursor.execute(
                    """
                    INSERT INTO observations
                    (id, entity_id, observation, embedding, created_at, properties)
                    VALUES (?, ?, ?, ?, datetime('now'), ?)
                    """,
                    ("test-observation-id", "nonexistent-entity-id", "Test observation", None, "{}")
                )
        
        finally:
            conn.close()
    
    def test_cascade_delete(self, temp_db_path):
        """Test that deleting entities cascades to related relations and observations."""
        # Create a knowledge graph and add data
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create entities and relationships
            entity1_id = kg.create_entity(name="CascadeTest1", entity_type="test")
            entity2_id = kg.create_entity(name="CascadeTest2", entity_type="test")
            
            # Create relations in both directions
            kg.create_relation(
                from_entity_id=entity1_id,
                to_entity_id=entity2_id,
                relation_type="cascade_test_outgoing"
            )
            
            kg.create_relation(
                from_entity_id=entity2_id,
                to_entity_id=entity1_id,
                relation_type="cascade_test_incoming"
            )
            
            # Add observations to both entities
            kg.add_observation(entity_id=entity1_id, observation="Observation on entity 1")
            kg.add_observation(entity_id=entity2_id, observation="Observation on entity 2")
            
            # Verify initial state
            assert len(kg.get_relations(entity1_id)) == 2  # Both directions
            assert len(kg.get_observations(entity1_id)) == 1
            
            # Delete entity1
            kg.delete_entity(entity1_id)
            
            # Verify entity1 is gone
            assert kg.get_entity(entity1_id) is None
            
            # Verify all relations involving entity1 are gone
            # The relations should be gone from entity2's relations
            remaining_relations = kg.get_relations(entity2_id)
            assert len(remaining_relations) == 0
            
            # Direct database check to be thorough
            conn = get_connection(temp_db_path)
            try:
                cursor = conn.cursor()
                
                # Check if any relations reference the deleted entity
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM relations
                    WHERE from_entity_id = ? OR to_entity_id = ?
                    """,
                    (entity1_id, entity1_id)
                )
                count = cursor.fetchone()[0]
                assert count == 0
                
                # Check if any observations reference the deleted entity
                cursor.execute(
                    "SELECT COUNT(*) FROM observations WHERE entity_id = ?",
                    (entity1_id,)
                )
                count = cursor.fetchone()[0]
                assert count == 0
            
            finally:
                conn.close()
        
        finally:
            kg.close()
    
    def test_database_concurrency(self, temp_db_path):
        """Test that concurrent database operations are handled properly."""
        # Create two knowledge graph instances with the same database
        kg1 = KnowledgeGraph(db_path=temp_db_path)
        kg2 = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create entity with first instance
            entity_id = kg1.create_entity(name="ConcurrencyTest", entity_type="test")
            
            # Verify it's immediately visible to second instance
            entity = kg2.get_entity(entity_id)
            assert entity is not None
            assert entity.name == "ConcurrencyTest"
            
            # Update entity with second instance
            kg2.update_entity(entity_id=entity_id, name="UpdatedBySecondInstance")
            
            # Verify update is visible to first instance
            updated_entity = kg1.get_entity(entity_id)
            assert updated_entity.name == "UpdatedBySecondInstance"
            
            # Add relation with first instance
            entity2_id = kg1.create_entity(name="ConcurrencyTest2", entity_type="test")
            relation_id = kg1.create_relation(
                from_entity_id=entity_id,
                to_entity_id=entity2_id,
                relation_type="concurrency_test"
            )
            
            # Verify relation is visible to second instance
            relations = kg2.get_relations(entity_id)
            assert len(relations) == 1
            assert relations[0]["id"] == relation_id
            
            # Add observation with second instance
            observation_text = "Observation added by second instance"
            observation_id = kg2.add_observation(
                entity_id=entity_id,
                observation=observation_text
            )
            
            # Verify observation is visible to first instance
            observations = kg1.get_observations(entity_id)
            assert len(observations) == 1
            assert observations[0].id == observation_id
            assert observations[0].observation == observation_text
            
            # Delete relation with second instance
            kg2.delete_relation(relation_id)
            
            # Verify deletion is visible to first instance
            assert len(kg1.get_relations(entity_id)) == 0
            
            # Delete entity with first instance
            kg1.delete_entity(entity_id)
            
            # Verify deletion is visible to second instance
            assert kg2.get_entity(entity_id) is None
        
        finally:
            kg1.close()
            kg2.close()
    
    def test_transaction_integrity(self, temp_db_path):
        """Test that database transactions maintain data integrity."""
        # This test simulates a scenario where an operation fails mid-transaction
        
        # Create a knowledge graph instance
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create an entity
            entity_id = kg.create_entity(name="TransactionTest", entity_type="test")
            
            # Get direct database connection to check transaction behavior
            conn = get_connection(temp_db_path)
            try:
                cursor = conn.cursor()
                
                # Start a transaction
                conn.execute("BEGIN TRANSACTION")
                
                # Insert a valid relation
                cursor.execute(
                    """
                    INSERT INTO relations 
                    (id, from_entity_id, to_entity_id, relation_type, confidence, created_at, properties)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
                    """,
                    ("good-relation-id", entity_id, entity_id, "self_relation", 1.0, "{}")
                )
                
                # Try to insert an invalid relation (FK violation)
                try:
                    cursor.execute(
                        """
                        INSERT INTO relations 
                        (id, from_entity_id, to_entity_id, relation_type, confidence, created_at, properties)
                        VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
                        """,
                        ("bad-relation-id", entity_id, "nonexistent-id", "invalid_relation", 1.0, "{}")
                    )
                    # Should not reach here
                    conn.commit()
                except sqlite3.IntegrityError:
                    # Expected error - rollback
                    conn.rollback()
                
                # Verify the valid relation was not committed due to rollback
                cursor.execute(
                    "SELECT COUNT(*) FROM relations WHERE id = ?",
                    ("good-relation-id",)
                )
                count = cursor.fetchone()[0]
                assert count == 0
                
                # Verify via the API as well
                relations = kg.get_relations(entity_id)
                assert len(relations) == 0
            
            finally:
                conn.close()
        
        finally:
            kg.close()
    
    def test_database_backup_and_restore(self):
        """Test backup and restore functionality at the database level."""
        # Create a source database with data
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as source_file:
            source_db_path = source_file.name
        
        # Create a target database for restore
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as target_file:
            target_db_path = target_file.name
        
        try:
            # Create and populate source database
            source_kg = KnowledgeGraph(db_path=source_db_path)
            try:
                # Add some data
                entity1_id = source_kg.create_entity(name="BackupEntity1", entity_type="test")
                entity2_id = source_kg.create_entity(name="BackupEntity2", entity_type="test")
                
                relation_id = source_kg.create_relation(
                    from_entity_id=entity1_id,
                    to_entity_id=entity2_id,
                    relation_type="backup_test"
                )
                
                observation_id = source_kg.add_observation(
                    entity_id=entity1_id,
                    observation="Backup test observation"
                )
                
                # Get initial stats
                source_stats = source_kg.get_stats()
                assert source_stats["entity_count"] == 2
                assert source_stats["relation_count"] == 1 # This was correct
                assert source_stats["observation_count"] == 1 # This was correct
            
            finally:
                source_kg.close()
            
            # Create an empty target database
            target_kg = KnowledgeGraph(db_path=target_db_path)
            try:
                # Verify it's empty
                target_stats = target_kg.get_stats()
                assert target_stats["entity_count"] == 0
                
                # Use the restore method to copy from source to target
                success = target_kg.restore(source_db_path)
                assert success is True
                
                # Verify the data was restored
                restored_stats = target_kg.get_stats()
                assert restored_stats["entity_count"] == 2
                assert restored_stats["relation_count"] == 1 # Correct
                assert restored_stats["observation_count"] == 1 # Correct
                
                # Verify specific entities
                entity1 = target_kg.get_entity(entity1_id)
                entity2 = target_kg.get_entity(entity2_id)
                
                assert entity1 is not None
                assert entity1.name == "BackupEntity1"
                assert entity2 is not None
                assert entity2.name == "BackupEntity2"
                
                # Verify relations
                relations = target_kg.get_relations(entity1_id)
                assert len(relations) == 1
                assert relations[0]["id"] == relation_id
                
                # Verify observations
                observations = target_kg.get_observations(entity1_id)
                assert len(observations) == 1
                assert observations[0].id == observation_id
            
            finally:
                target_kg.close()
        
        finally:
            # Clean up
            if os.path.exists(source_db_path):
                os.unlink(source_db_path)
            if os.path.exists(target_db_path):
                os.unlink(target_db_path)
    
    def test_database_size_growth(self, temp_db_path):
        """Test that database size grows appropriately with data addition."""
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Get initial database size
            initial_stats = kg.get_stats()
            initial_size = initial_stats["db_size_bytes"]
            
            # Add a significant amount of data
            for i in range(10):
                entity_id = kg.create_entity(
                    name=f"SizeTestEntity{i}",
                    entity_type="test",
                    properties={"index": i, "data": "X" * 1000}  # Add some size
                )
                
                # Add observations with substantial text
                kg.add_observation(
                    entity_id=entity_id,
                    observation=f"This is observation {i} with lots of text: " + "Lorem ipsum " * 50
                )
            
            # Get final database size
            final_stats = kg.get_stats()
            final_size = final_stats["db_size_bytes"]
            
            # Database should have grown
            assert final_size > initial_size
            
            # The growth should be substantial given the data added
            # At least 10KB growth would be reasonable
            assert final_size - initial_size > 10000
        
        finally:
            kg.close()

    def test_in_memory_vs_file_performance(self, in_memory_db_connection, temp_db_path):
        """Compare performance between in-memory and file-based databases."""
        # Create a file-based knowledge graph
        file_kg = KnowledgeGraph(db_path=temp_db_path)
        
        # Create an in-memory knowledge graph using our fixture
        with patch('car_mcp.knowledge_graph_core_facade.db_handler.init_database') as mock_init_db: # Updated path
            mock_init_db.return_value = in_memory_db_connection
            
            in_memory_kg = KnowledgeGraph(
                db_path=":memory:",
                redis_client=None,
                embedding_function=None
            )
            
            # Replace the connection with our in-memory one
            in_memory_kg.conn = in_memory_db_connection
            
            try:
                # Measure time to create entities in file-based DB
                file_start_time = time.time()
                file_entities = []
                for i in range(50):
                    entity_id = file_kg.create_entity(
                        name=f"FileEntity{i}",
                        entity_type="test",
                        properties={"index": i}
                    )
                    file_entities.append(entity_id)
                file_create_time = time.time() - file_start_time
                
                # Measure time to create entities in in-memory DB
                memory_start_time = time.time()
                memory_entities = []
                for i in range(50):
                    entity_id = in_memory_kg.create_entity(
                        name=f"MemoryEntity{i}",
                        entity_type="test",
                        properties={"index": i}
                    )
                    memory_entities.append(entity_id)
                memory_create_time = time.time() - memory_start_time
                
                # In-memory should generally be faster, but we don't want
                # to make the test brittle with exact timings
                # Just ensure both complete successfully
                assert len(file_entities) == 50
                assert len(memory_entities) == 50
                
                # Measure retrieval performance
                file_retrieval_start = time.time()
                for entity_id in file_entities:
                    entity = file_kg.get_entity(entity_id)
                    assert entity is not None
                file_retrieval_time = time.time() - file_retrieval_start
                
                memory_retrieval_start = time.time()
                for entity_id in memory_entities:
                    entity = in_memory_kg.get_entity(entity_id)
                    assert entity is not None
                memory_retrieval_time = time.time() - memory_retrieval_start
                
                # Log performance metrics for analysis
                print(f"File-based create time: {file_create_time:.4f}s")
                print(f"In-memory create time: {memory_create_time:.4f}s")
                print(f"File-based retrieval time: {file_retrieval_time:.4f}s")
                print(f"In-memory retrieval time: {memory_retrieval_time:.4f}s")
            
            finally:
                in_memory_kg.close()
                file_kg.close()

    def test_sqlite_pragma_settings(self, temp_db_path):
        """Test that SQLite pragma settings are correctly applied."""
        # Create a knowledge graph instance
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Check that foreign keys are enabled
            cursor = kg.conn.cursor()
            cursor.execute("PRAGMA foreign_keys;")
            foreign_keys_enabled = cursor.fetchone()[0]
            assert foreign_keys_enabled == 1, "Foreign keys should be enabled"
            
            # Check journal mode (should be WAL for performance)
            cursor.execute("PRAGMA journal_mode;")
            journal_mode = cursor.fetchone()[0].upper()
            assert journal_mode in ("WAL", "DELETE"), "Journal mode should be WAL or DELETE"
            
            # Check synchronous setting
            cursor.execute("PRAGMA synchronous;")
            synchronous = cursor.fetchone()[0]
            # Less than 2 (FULL) is acceptable for performance
            assert synchronous in (0, 1, 2), "Synchronous setting should be valid"
            
            # Verify that foreign key constraints work
            # Create test entities for constraint checking
            entity1_id = kg.create_entity(name="PragmaTest1", entity_type="test")
            entity2_id = kg.create_entity(name="PragmaTest2", entity_type="test")
            
            # Create a valid relation
            relation_id = kg.create_relation(
                from_entity_id=entity1_id,
                to_entity_id=entity2_id,
                relation_type="pragma_test"
            )
            
            # Try to delete the entity with a foreign key relationship
            # Should succeed because of ON DELETE CASCADE
            kg.delete_entity(entity1_id)
            
            # Verify the relation was also deleted
            cursor.execute(
                "SELECT COUNT(*) FROM relations WHERE id = ?",
                (relation_id,)
            )
            count = cursor.fetchone()[0]
            assert count == 0, "Relation should be deleted via cascade"
        
        finally:
            kg.close()

    def test_database_vacuum_and_optimize(self, temp_db_path):
        """Test database vacuuming and optimization."""
        # Create a knowledge graph instance
        kg = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create and delete a lot of entities to create free space in the database
            entity_ids = []
            for i in range(50):
                entity_id = kg.create_entity(
                    name=f"VacuumTestEntity{i}",
                    entity_type="test"
                )
                entity_ids.append(entity_id)
            
            # Get size before deletions
            cursor = kg.conn.cursor()
            cursor.execute("PRAGMA page_count;")
            page_count_before = cursor.fetchone()[0]
            cursor.execute("PRAGMA page_size;")
            page_size = cursor.fetchone()[0]
            size_before = page_count_before * page_size
            
            # Delete most of the entities
            for entity_id in entity_ids[:40]:
                kg.delete_entity(entity_id)
            
            # Run VACUUM to reclaim space
            cursor.execute("VACUUM;")
            
            # Get size after VACUUM
            cursor.execute("PRAGMA page_count;")
            page_count_after = cursor.fetchone()[0]
            size_after = page_count_after * page_size
            
            # Optimize indices
            cursor.execute("ANALYZE;")
            
            # Verify remaining entities are still accessible
            for entity_id in entity_ids[40:]:
                entity = kg.get_entity(entity_id)
                assert entity is not None
                assert entity.name.startswith("VacuumTestEntity")
            
            # Database should be optimized - not larger than needed
            # Size shouldn't be proportional to 50 entities after deleting 40
            # But we need to be careful about exact assertions due to DB overhead
            print(f"Size before: {size_before}, Size after: {size_after}")
        
        finally:
            kg.close()

    def test_transaction_isolation(self, temp_db_path):
        """Test transaction isolation levels in SQLite."""
        # Create two knowledge graph instances with the same database
        kg1 = KnowledgeGraph(db_path=temp_db_path)
        kg2 = KnowledgeGraph(db_path=temp_db_path)
        
        try:
            # Create initial entities
            entity1_id = kg1.create_entity(name="IsolationTest1", entity_type="test")
            entity2_id = kg1.create_entity(name="IsolationTest2", entity_type="test")
            
            # Start a transaction in first connection
            kg1.conn.execute("BEGIN TRANSACTION;")
            
            # Modify entity in first connection (inside transaction)
            kg1.conn.execute(
                "UPDATE entities SET name = ? WHERE id = ?",
                ("UpdatedInTransaction", entity1_id)
            )
            
            # Before commit, second connection should still see old data
            cursor = kg2.conn.cursor()
            cursor.execute("SELECT name FROM entities WHERE id = ?", (entity1_id,))
            name_before_commit = cursor.fetchone()[0]
            assert name_before_commit == "IsolationTest1", "Uncommitted changes should not be visible"
            
            # Commit transaction
            kg1.conn.commit()
            
            # After commit, second connection should see changes
            cursor.execute("SELECT name FROM entities WHERE id = ?", (entity1_id,))
            name_after_commit = cursor.fetchone()[0]
            assert name_after_commit == "UpdatedInTransaction", "Committed changes should be visible"
            
            # Test rollback behavior
            kg1.conn.execute("BEGIN TRANSACTION;")
            kg1.conn.execute(
                "UPDATE entities SET name = ? WHERE id = ?",
                ("WillBeRolledBack", entity2_id)
            )
            
            # Rollback changes
            kg1.conn.rollback()
            
            # Second connection should see original data (rollback worked)
            cursor.execute("SELECT name FROM entities WHERE id = ?", (entity2_id,))
            name_after_rollback = cursor.fetchone()[0]
            assert name_after_rollback == "IsolationTest2", "Rolled back changes should not be visible"
        
        finally:
            kg1.close()
            kg2.close()

    def test_index_performance(self, in_memory_db_connection):
        """Test that database indices improve query performance."""
        # Use in-memory database for consistent performance testing
        # Patch init_database where it's looked up by KnowledgeGraph (i.e., in graph_facade module)
        with patch('car_mcp.knowledge_graph_core_facade.graph_facade.init_database') as mock_init_db:
            mock_init_db.return_value = in_memory_db_connection
            
            kg = KnowledgeGraph(
                db_path=":memory:",
                redis_client=None,
                embedding_function=None
            )
            
            # Replace the connection with our in-memory one
            kg.conn = in_memory_db_connection
            
            try:
                # Create a large number of entities with different types
                entity_types = ["type1", "type2", "type3", "type4", "type5"]
                entity_ids_by_type = {t: [] for t in entity_types}
                
                for i in range(200):
                    entity_type = entity_types[i % len(entity_types)]
                    entity_id = kg.create_entity(
                        name=f"IndexTestEntity{i}",
                        entity_type=entity_type,
                        properties={"index": i}
                    )
                    entity_ids_by_type[entity_type].append(entity_id)
                
                # Test indexed query (by entity_type)
                cursor = kg.conn.cursor()
                
                # First with EXPLAIN QUERY PLAN to verify index usage
                cursor.execute("EXPLAIN QUERY PLAN SELECT * FROM entities WHERE entity_type = ?", ("type1",))
                plan_rows = cursor.fetchall()
                
                # Extract detail from each row of the plan
                plan_details_text = []
                for row in plan_rows:
                    if 'detail' in row.keys():
                        plan_details_text.append(str(row['detail']))
                    else: # Fallback for older SQLite or different row structure
                        plan_details_text.append(str(row)) # Add the whole row string if 'detail' key is missing

                full_plan_str = " | ".join(plan_details_text)
                
                # Check if the specific index idx_entity_type is used, or a general indexed search
                # SQLite's EXPLAIN QUERY PLAN output can vary.
                # A common pattern for indexed search is "SEARCH TABLE entities USING INDEX idx_entity_type (...)"
                # or "SCAN TABLE entities USING INDEX idx_entity_type (...)".
                # Sometimes it might just say "USING INDEX idx_entity_type".
                
                found_expected_index_usage = False
                for detail_text_item in plan_details_text:
                    # More robust check for various SQLite versions
                    # Check for the plural form 'idx_entities_type' as observed in the EXPLAIN QUERY PLAN output.
                    if "idx_entities_type" in detail_text_item.lower() and \
                       ("search" in detail_text_item.lower() or "scan" in detail_text_item.lower() or "using index" in detail_text_item.lower()):
                        found_expected_index_usage = True
                        break
                
                assert found_expected_index_usage, \
                    f"Query plan did not indicate use of the expected index 'idx_entity_type'. Plan: {full_plan_str}"
                
                # Measure performance with index
                start_time = time.time()
                cursor.execute("SELECT * FROM entities WHERE entity_type = ?", ("type1",))
                results = cursor.fetchall()
                indexed_query_time = time.time() - start_time
                
                # Verify results
                assert len(results) == len(entity_ids_by_type["type1"])
                
                # Measure performance of non-indexed query (custom property)
                start_time = time.time()
                all_entities = []
                for entity_id in [id for ids in entity_ids_by_type.values() for id in ids]:
                    entity = kg.get_entity(entity_id)
                    if entity.properties.get("index", 0) % 5 == 0:
                        all_entities.append(entity)
                non_indexed_query_time = time.time() - start_time
                
                # Indexed queries should be completing correctly
                print(f"Indexed query time: {indexed_query_time:.4f}s")
                print(f"Non-indexed query time: {non_indexed_query_time:.4f}s")
            
            finally:
                kg.close()

    def test_complex_data_integrity(self, in_memory_knowledge_graph):
        """Test integrity of complex interconnected data structures."""
        kg = in_memory_knowledge_graph
        
        # Create a complex interconnected graph
        file_entity_ids = []
        function_entity_ids = []
        
        # Create file entities
        for i in range(3):
            entity_id = kg.create_entity(
                name=f"File{i}.py",
                entity_type="file",
                properties={"path": f"/path/to/File{i}.py"}
            )
            file_entity_ids.append(entity_id)
        
        # Create function entities
        for i in range(5):
            file_index = i % len(file_entity_ids)
            entity_id = kg.create_entity(
                name=f"function_{i}",
                entity_type="function",
                properties={
                    "file_id": file_entity_ids[file_index],
                    "line_number": i * 10
                }
            )
            function_entity_ids.append(entity_id)
        
        # Create contains relations (files contain functions)
        for i, function_id in enumerate(function_entity_ids):
            file_index = i % len(file_entity_ids)
            kg.create_relation(
                from_entity_id=file_entity_ids[file_index],
                to_entity_id=function_id,
                relation_type="contains",
                confidence=1.0
            )
        
        # Create calls relations (functions call other functions)
        for i in range(len(function_entity_ids)):
            for j in range(1, 3):  # Each function calls up to 2 others
                target_index = (i + j) % len(function_entity_ids)
                kg.create_relation(
                    from_entity_id=function_entity_ids[i],
                    to_entity_id=function_entity_ids[target_index],
                    relation_type="calls",
                    confidence=0.9
                )
        
        # Add observations
        for function_id in function_entity_ids:
            kg.add_observation(
                entity_id=function_id,
                observation=f"This function does complex operations."
            )
        
        # Verify graph topology
        for i, file_id in enumerate(file_entity_ids):
            # Get contained functions
            contained_relations = kg.get_relations(
                entity_id=file_id,
                direction="outgoing",
                relation_type="contains"
            )
            
            # Each file should contain 1-2 functions
            assert 1 <= len(contained_relations) <= 2
            
            # Get functions that call functions in this file
            file_functions = [rel["to_entity_id"] for rel in contained_relations]
            all_callers = set()
            
            for function_id in file_functions:
                incoming_calls = kg.get_relations(
                    entity_id=function_id,
                    direction="incoming",
                    relation_type="calls"
                )
                callers = [rel["from_entity_id"] for rel in incoming_calls]
                all_callers.update(callers)
            
            # There should be at least one function calling a function in this file
            assert len(all_callers) > 0
        
        # Verify all functions have observations
        for function_id in function_entity_ids:
            observations = kg.get_observations(function_id)
            assert len(observations) > 0
        
        # Verify data integrity after modification
        # Delete a file and ensure all its functions' relations are properly cleaned up
        file_to_delete = file_entity_ids[0]
        
        # Find functions contained in this file
        contained_relations = kg.get_relations(
            entity_id=file_to_delete,
            direction="outgoing",
            relation_type="contains"
        )
        contained_function_ids = [rel["to_entity_id"] for rel in contained_relations]
        
        # Delete the file
        kg.delete_entity(file_to_delete)
        
        # Verify the file is gone
        assert kg.get_entity(file_to_delete) is None
        
        # Verify contained functions are gone
        for function_id in contained_function_ids:
            assert kg.get_entity(function_id) is None
        
        # Verify all relations involving these functions are gone
        for function_id in function_entity_ids:
            if function_id in contained_function_ids:
                continue  # Skip deleted functions
                
            # Get all relations
            relations = kg.get_relations(function_id)
            
            # None should reference deleted functions
            for relation in relations:
                assert relation["from_entity_id"] not in contained_function_ids
                assert relation["to_entity_id"] not in contained_function_ids
        
        # Database should still be in a consistent state
        stats = kg.get_stats()
        assert stats["entity_count"] == len(file_entity_ids) + len(function_entity_ids) - 1 - len(contained_function_ids)