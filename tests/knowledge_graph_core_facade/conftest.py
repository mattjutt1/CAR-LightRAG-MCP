"""
Knowledge Graph specific test fixtures.

This module provides specialized fixtures for testing the Knowledge Graph component
with a focus on modern testing practices and proper dependency isolation.
"""

import json
import os
import sys
import sqlite3
import tempfile
import importlib
import logging # Added for logger_conftest
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator, Callable, Protocol, runtime_checkable
from unittest.mock import MagicMock, patch

import pytest

# Import models directly since they don't have external dependencies
from car_mcp.knowledge_graph_core_facade.kg_models_all import Entity, Relation, Observation, CacheProvider, ContextLogger
from car_mcp.knowledge_graph_core_facade.db_handler import init_database
from car_mcp.knowledge_graph_core_facade.graph_facade import KnowledgeGraph


class MockCacheProvider:
    """Mock implementation of the CacheProvider protocol for testing."""
    
    def __init__(self):
        self.store = {}
        self.keys_called = []
        self.set_calls = []
        self.delete_calls = []
    
    def get(self, key):
        """Get a value from the mock cache."""
        return self.store.get(key)
    
    def set(self, key, value, ex=None):
        """Set a value in the mock cache."""
        self.store[key] = value
        self.set_calls.append((key, value, ex))
        return True
    
    def delete(self, *keys):
        """Delete keys from the mock cache."""
        deleted = 0
        self.delete_calls.append(keys)
        for key in keys:
            if key in self.store:
                del self.store[key]
                deleted += 1
        return deleted
    
    def exists(self, key):
        """Check if a key exists in the mock cache."""
        return key in self.store
    
    def keys(self, pattern=None):
        """Get keys matching a pattern."""
        self.keys_called.append(pattern)
        if pattern and '*' in pattern:
            prefix = pattern.split('*')[0]
            return [k for k in self.store.keys() if k.startswith(prefix)]
        return list(self.store.keys())


class MockContextLogger:
    """Mock implementation of the ContextLogger protocol for testing."""
    
    def __init__(self):
        self.events = []
    
    def log_event(self, event_name, data=None):
        """Log an event with optional data."""
        self.events.append((event_name, data))
        return None


class MockEmbeddingFunction:
    """Mock implementation of the EmbeddingFunction protocol for testing."""
    
    def __init__(self, vector_size=10):
        self.called_with = []
        self.vector_size = vector_size
    
    def __call__(self, text):
        """Generate a deterministic mock embedding based on the text."""
        import hashlib
        self.called_with.append(text)
        hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
        # Generate a vector of the specified size for consistency
        return [(hash_val % 1000) / 1000.0 + i * 0.1 for i in range(self.vector_size)]


@pytest.fixture
def mock_cache_provider():
    """Fixture providing a mock cache provider that implements the CacheProvider protocol."""
    return MockCacheProvider()


@pytest.fixture
def mock_context_logger():
    """Fixture providing a mock context logger that implements the ContextLogger protocol."""
    return MockContextLogger()


@pytest.fixture
def mock_embedding_function():
    """Fixture providing a mock embedding function that implements the EmbeddingFunction protocol."""
    return MockEmbeddingFunction()


@pytest.fixture
def knowledge_graph_class():
    """
    Fixture that returns the KnowledgeGraph class without instantiating it.
    
    This allows tests to mock or patch aspects of the class before creating instances.
    """
    return KnowledgeGraph


@pytest.fixture
def knowledge_graph(temp_db_path, mock_cache_provider, mock_context_logger, mock_embedding_function):
    """
    Fixture providing a configured KnowledgeGraph instance with mock dependencies.
    
    This uses the get_knowledge_graph_class function to lazily import the KnowledgeGraph class,
    avoiding the direct import that would cause FastMCP dependency errors in tests.
    """
    # Patch any potentially problematic imports that might occur during KnowledgeGraph instantiation
    with patch.dict('sys.modules', {'fastmcp': MagicMock()}):
        # KnowledgeGraph is already imported directly
        kg = KnowledgeGraph(
            db_path=temp_db_path,
            redis_client=mock_cache_provider,
            context_logger=mock_context_logger,
            embedding_function=mock_embedding_function,
            cache_ttl=60
        )
        yield kg

@pytest.fixture(scope="function")
def in_memory_db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Fixture providing an in-memory SQLite database connection for tests.
    
    This uses SQLite's special :memory: database which exists only in memory,
    providing fast and isolated test databases.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    
    # Initialize the schema
    cursor = conn.cursor()
    
    # Create entities table
    cursor.execute("""
    CREATE TABLE entities (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        properties TEXT,
        embedding TEXT
    )
    """)
    
    # Create observations table
    cursor.execute("""
    CREATE TABLE observations (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL,
        observation TEXT NOT NULL,
        created_at TEXT NOT NULL,
        properties TEXT,
        embedding TEXT,
        FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE
    )
    """)
    
    # Create relations table
    cursor.execute("""
    CREATE TABLE relations (
        id TEXT PRIMARY KEY,
        from_entity_id TEXT NOT NULL,
        to_entity_id TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        confidence REAL NOT NULL,
        created_at TEXT NOT NULL,
        properties TEXT,
        FOREIGN KEY (from_entity_id) REFERENCES entities (id) ON DELETE CASCADE,
        FOREIGN KEY (to_entity_id) REFERENCES entities (id) ON DELETE CASCADE
    )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX idx_entities_name ON entities (name)")
    cursor.execute("CREATE INDEX idx_entities_type ON entities (entity_type)")
    cursor.execute("CREATE INDEX idx_observations_entity_id ON observations (entity_id)")
    cursor.execute("CREATE INDEX idx_relations_from_entity_id ON relations (from_entity_id)")
    cursor.execute("CREATE INDEX idx_relations_to_entity_id ON relations (to_entity_id)")
    cursor.execute("CREATE INDEX idx_relations_type ON relations (relation_type)")
    
    conn.commit()
    
    yield conn
    
    # Cleanup is automatic for in-memory databases when connection is closed
    conn.close()


@pytest.fixture(scope="function")
def temp_db_path() -> Generator[str, None, None]:
    """Fixture providing a temporary database path for tests.
    
    Creates a temporary file for use as a SQLite database.
    The file is automatically removed after the test.
    """
    # Create a temporary file, get its path, and close it immediately.
    # The file itself will persist until explicitly deleted.
    tmp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp_file.name
    tmp_file.close()  # Ensure the file handle is closed before yielding the path
    
    yield db_path
    
    # Clean up after tests
    if os.path.exists(db_path):
        try:
            os.unlink(db_path)
        except PermissionError:
            # On Windows, sometimes the file is still locked briefly.
            # Add a small delay and retry, or log a warning if it persists.
            # For now, we'll just let it fail if it's truly locked by something else.
            # Ideally, all connections should be closed by the test/fixtures using this path.
            logger_conftest = logging.getLogger(__name__)
            logger_conftest.warning(f"Could not delete temp_db_path {db_path} due to PermissionError. It might still be in use.")
            # If critical, could add a small time.sleep() and retry, but that can make tests flaky.
            pass # Or re-raise if strict cleanup is needed.


@pytest.fixture(scope="function")
def db_connection(temp_db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """Fixture providing a SQLite database connection for tests.
    
    Uses a temporary file database that is automatically initialized with
    the Knowledge Graph schema.
    """
    conn = init_database(temp_db_path)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def enhanced_mock_redis_client() -> MagicMock:
    """Fixture providing an enhanced mock Redis client for tests.
    
    This mock implements more Redis functionality including hash operations,
    list operations, and proper key management for more realistic testing.
    """
    redis_mock = MagicMock()
    
    # Dictionary to simulate Redis storage
    mock_store = {}
    mock_hash_store = {}
    mock_list_store = {}
    mock_ttls = {}
    
    # Implement basic get/set functionality
    def mock_get(key: str) -> Optional[str]:
        return mock_store.get(key)
    
    def mock_set(key: str, value: str, ex: Optional[int] = None, px: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        if nx and key in mock_store:
            return False
        if xx and key not in mock_store:
            return False
        
        mock_store[key] = value
        
        # Handle expiration
        if ex is not None:
            mock_ttls[key] = ex
        elif px is not None:
            mock_ttls[key] = px / 1000
            
        return True
    
    def mock_exists(key: str) -> bool:
        return key in mock_store
    
    def mock_delete(*keys: str) -> int:
        count = 0
        for key in keys:
            if key in mock_store:
                del mock_store[key]
                if key in mock_ttls:
                    del mock_ttls[key]
                count += 1
        return count
    
    # Implement hash operations
    def mock_hget(key: str, field: str) -> Optional[str]:
        if key not in mock_hash_store:
            return None
        return mock_hash_store[key].get(field)
    
    def mock_hset(key: str, field: str, value: str) -> int:
        if key not in mock_hash_store:
            mock_hash_store[key] = {}
        
        is_new = field not in mock_hash_store[key]
        mock_hash_store[key][field] = value
        return 1 if is_new else 0
    
    def mock_hmset(key: str, mapping: Dict[str, str]) -> bool:
        if key not in mock_hash_store:
            mock_hash_store[key] = {}
        
        mock_hash_store[key].update(mapping)
        return True
    
    def mock_hgetall(key: str) -> Dict[str, str]:
        if key not in mock_hash_store:
            return {}
        return mock_hash_store[key]
    
    # Implement list operations
    def mock_lpush(key: str, *values: str) -> int:
        if key not in mock_list_store:
            mock_list_store[key] = []
        
        for value in values:
            mock_list_store[key].insert(0, value)
        
        return len(mock_list_store[key])
    
    def mock_rpush(key: str, *values: str) -> int:
        if key not in mock_list_store:
            mock_list_store[key] = []
        
        for value in values:
            mock_list_store[key].append(value)
        
        return len(mock_list_store[key])
    
    def mock_lrange(key: str, start: int, end: int) -> List[str]:
        if key not in mock_list_store:
            return []
        
        # Handle negative indices
        if end == -1:
            end = len(mock_list_store[key])
        
        return mock_list_store[key][start:end]
    
    # Configure the mock
    redis_mock.get.side_effect = mock_get
    redis_mock.set.side_effect = mock_set
    redis_mock.exists.side_effect = mock_exists
    redis_mock.delete.side_effect = mock_delete
    
    redis_mock.hget.side_effect = mock_hget
    redis_mock.hset.side_effect = mock_hset
    redis_mock.hmset.side_effect = mock_hmset
    redis_mock.hgetall.side_effect = mock_hgetall
    
    redis_mock.lpush.side_effect = mock_lpush
    redis_mock.rpush.side_effect = mock_rpush
    redis_mock.lrange.side_effect = mock_lrange

    # Add keys method for enhanced mock as well
    def mock_keys_enhanced(pattern: Optional[str] = None) -> List[str]:
        # This mock_store is local to enhanced_mock_redis_client's setup
        # Need to ensure it refers to the correct store if they are different
        # For enhanced_mock_redis_client, the primary store is 'mock_store' (line 267)
        if pattern and '*' in pattern:
            prefix = pattern.split('*')[0]
            return [k for k in mock_store.keys() if k.startswith(prefix)]
        elif pattern:
            return [k for k in mock_store.keys() if k == pattern]
        return list(mock_store.keys())

    redis_mock.keys.side_effect = mock_keys_enhanced
    
    return redis_mock


@pytest.fixture(scope="function")
def mock_redis_client() -> MagicMock:
    """Fixture providing a basic mock Redis client for tests.
    
    For more comprehensive Redis mocking, use the enhanced_mock_redis_client fixture.
    """
    redis_mock = MagicMock()
    
    # Dictionary to simulate Redis storage
    mock_store = {}
    
    # Implement basic get/set functionality
    def mock_get(key: str) -> Optional[str]:
        return mock_store.get(key)
    
    def mock_set(key: str, value: str, ex: Optional[int] = None) -> bool:
        mock_store[key] = value
        return True
    
    def mock_exists(key: str) -> bool:
        return key in mock_store
    
    def mock_delete(*keys_arg: str) -> int: # Renamed to avoid conflict with outer scope 'keys' if any
        count = 0
        for key_item in keys_arg:
            if key_item in mock_store:
                del mock_store[key_item]
                # Also remove from TTLs if present, like in enhanced_mock_redis_client
                # if key_item in mock_ttls: # mock_ttls is not defined in this simpler mock
                #     del mock_ttls[key_item]
                count += 1
        return count

    def mock_keys(pattern: Optional[str] = None) -> List[str]:
        if pattern and '*' in pattern:
            # Basic glob-style matching for prefix*
            prefix = pattern.split('*')[0]
            return [k for k in mock_store.keys() if k.startswith(prefix)]
        elif pattern: # Exact match if no wildcard
            return [k for k in mock_store.keys() if k == pattern]
        return list(mock_store.keys())

    redis_mock.get.side_effect = mock_get
    redis_mock.set.side_effect = mock_set
    redis_mock.exists.side_effect = mock_exists
    redis_mock.delete.side_effect = mock_delete
    redis_mock.keys.side_effect = mock_keys # Add keys method

    return redis_mock


@pytest.fixture(scope="function")
def deterministic_embedding_function() -> Callable[[str], List[float]]:
    """Fixture providing a deterministic embedding function for tests.
    
    This function generates consistent embeddings based on the input text,
    making tests predictable and reproducible.
    """
    def generate_deterministic_embedding(text: str) -> List[float]:
        """Generate a deterministic embedding vector based on the input text."""
        import hashlib
        # Create a consistent hash of the text
        hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
        # Generate a 10-dimensional embedding vector
        return [(hash_val % 1000) / 1000.0 + i * 0.1 for i in range(10)]
    
    return generate_deterministic_embedding


@pytest.fixture(scope="function")
def mock_embedding_function(deterministic_embedding_function: Callable[[str], List[float]]) -> Callable[[str], List[float]]:
    """Fixture providing a mock embedding function for tests."""
    return deterministic_embedding_function


@pytest.fixture(scope="function")
def mock_context_logger() -> MagicMock:
    """Fixture providing a mock context logger for tests."""
    logger_mock = MagicMock()
    logger_mock.log_event.return_value = None
    return logger_mock


@pytest.fixture(scope="function")
def knowledge_graph(temp_db_path: str, mock_redis_client: MagicMock, 
                   mock_embedding_function: Callable, 
                   mock_context_logger: MagicMock) -> Generator[KnowledgeGraph, None, None]:
    """Fixture providing a configured Knowledge Graph instance for tests."""
    kg = KnowledgeGraph(
        db_path=temp_db_path,
        redis_client=mock_redis_client,
        embedding_function=mock_embedding_function,
        context_logger=mock_context_logger
    )
    yield kg
    kg.close()


@pytest.fixture(scope="function")
def in_memory_knowledge_graph(in_memory_db_connection: sqlite3.Connection, 
                             enhanced_mock_redis_client: MagicMock,
                             mock_embedding_function: Callable, 
                             mock_context_logger: MagicMock) -> Generator[KnowledgeGraph, None, None]:
    """Fixture providing a Knowledge Graph instance with an in-memory database.
    
    This fixture is faster than the regular knowledge_graph fixture since it
    uses an in-memory database instead of a file-based one.
    """
    # Create a temporary path for the KnowledgeGraph constructor
    with tempfile.NamedTemporaryFile() as tmp:
        # Create a KnowledgeGraph with the connection but using an in-memory DB
        # Patch init_database where it's looked up by KnowledgeGraph (i.e., in graph_facade module)
        with patch('car_mcp.knowledge_graph_core_facade.graph_facade.init_database') as mock_init_db:
            mock_init_db.return_value = in_memory_db_connection
            
            # KnowledgeGraph is already imported directly
            # Pass tmp.name for db_path; the mocked init_database will ignore it and return the in-memory connection.
            kg = KnowledgeGraph(
                db_path=tmp.name,
                redis_client=enhanced_mock_redis_client,
                embedding_function=mock_embedding_function,
                context_logger=mock_context_logger
            )
            
            # If the patch worked, kg.conn should already be in_memory_db_connection.
            # This assertion helps verify the patch.
            assert kg.conn == in_memory_db_connection, "Patching init_database in graph_facade did not work as expected for in_memory_knowledge_graph."
            
            yield kg
            
            # Clean up
            kg.close()


@pytest.fixture(scope="function")
def sample_entity_data() -> Dict[str, Any]:
    """Fixture providing sample entity data for tests."""
    return {
        "name": "TestFunction",
        "entity_type": "function",
        "properties": {
            "language": "python",
            "file_path": "/path/to/test.py",
            "line_number": 42
        }
    }


@pytest.fixture(scope="function")
def sample_relation_data() -> Dict[str, Any]:
    """Fixture providing sample relation data for tests."""
    return {
        "relation_type": "calls",
        "confidence": 0.95,
        "properties": {
            "count": 3,
            "locations": [45, 67, 89]
        }
    }


@pytest.fixture(scope="function")
def sample_observation_data() -> Dict[str, Any]:
    """Fixture providing sample observation data for tests."""
    return {
        "observation": "This function implements the core algorithm for processing data.",
        "properties": {
            "source": "documentation",
            "confidence": 0.9
        }
    }


@pytest.fixture(scope="function")
def populated_knowledge_graph(knowledge_graph: KnowledgeGraph, 
                             sample_entity_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fixture providing a Knowledge Graph with sample data.
    
    This fixture creates entities, relations, and observations for testing.
    """
    # Create a few entities
    entity1_id = knowledge_graph.create_entity(
        name=sample_entity_data["name"],
        entity_type=sample_entity_data["entity_type"],
        properties=sample_entity_data["properties"]
    )
    
    entity2_id = knowledge_graph.create_entity(
        name="AnotherClass",
        entity_type="class",
        properties={"language": "python"}
    )
    
    entity3_id = knowledge_graph.create_entity(
        name="TestFile",
        entity_type="file",
        properties={"path": "/path/to/test.py"}
    )
    
    # Create relations between entities
    relation1_id = knowledge_graph.create_relation(
        from_entity_id=entity1_id,
        to_entity_id=entity2_id,
        relation_type="calls",
        confidence=0.95
    )
    
    relation2_id = knowledge_graph.create_relation(
        from_entity_id=entity3_id,
        to_entity_id=entity1_id,
        relation_type="contains",
        confidence=1.0
    )
    
    # Add observations to entities
    observation1_id = knowledge_graph.add_observation(
        entity_id=entity1_id,
        observation="This function is the main entry point for processing data."
    )
    
    observation2_id = knowledge_graph.add_observation(
        entity_id=entity2_id,
        observation="This class implements a key algorithm for data transformation."
    )
    
    # Return the graph and the IDs of created objects for test use
    return {
        "graph": knowledge_graph,
        "entities": {
            "entity1_id": entity1_id,
            "entity2_id": entity2_id,
            "entity3_id": entity3_id
        },
        "relations": {
            "relation1_id": relation1_id,
            "relation2_id": relation2_id
        },
        "observations": {
            "observation1_id": observation1_id,
            "observation2_id": observation2_id
        }
    }


@pytest.fixture(scope="function")
def populated_in_memory_knowledge_graph(in_memory_knowledge_graph: KnowledgeGraph, 
                                       sample_entity_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fixture providing an in-memory Knowledge Graph with sample data.
    
    This is the in-memory version of the populated_knowledge_graph fixture,
    providing faster test execution.
    """
    # Create a few entities
    entity1_id = in_memory_knowledge_graph.create_entity(
        name=sample_entity_data["name"],
        entity_type=sample_entity_data["entity_type"],
        properties=sample_entity_data["properties"]
    )
    
    entity2_id = in_memory_knowledge_graph.create_entity(
        name="AnotherClass",
        entity_type="class",
        properties={"language": "python"}
    )
    
    entity3_id = in_memory_knowledge_graph.create_entity(
        name="TestFile",
        entity_type="file",
        properties={"path": "/path/to/test.py"}
    )
    
    # Create relations between entities
    relation1_id = in_memory_knowledge_graph.create_relation(
        from_entity_id=entity1_id,
        to_entity_id=entity2_id,
        relation_type="calls",
        confidence=0.95
    )
    
    relation2_id = in_memory_knowledge_graph.create_relation(
        from_entity_id=entity3_id,
        to_entity_id=entity1_id,
        relation_type="contains",
        confidence=1.0
    )
    
    # Add observations to entities
    observation1_id = in_memory_knowledge_graph.add_observation(
        entity_id=entity1_id,
        observation="This function is the main entry point for processing data."
    )
    
    observation2_id = in_memory_knowledge_graph.add_observation(
        entity_id=entity2_id,
        observation="This class implements a key algorithm for data transformation."
    )
    
    # Return the graph and the IDs of created objects for test use
    return {
        "graph": in_memory_knowledge_graph,
        "entities": {
            "entity1_id": entity1_id,
            "entity2_id": entity2_id,
            "entity3_id": entity3_id
        },
        "relations": {
            "relation1_id": relation1_id,
            "relation2_id": relation2_id
        },
        "observations": {
            "observation1_id": observation1_id,
            "observation2_id": observation2_id
        }
    }


@pytest.fixture(scope="function")
def backup_dir() -> Generator[str, None, None]:
    """Fixture providing a temporary directory for backup/restore tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir