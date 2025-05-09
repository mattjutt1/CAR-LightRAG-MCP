"""
Test fixtures for the CAR/LightRAG MCP server tests.

This module contains shared fixtures used across different test modules.
It uses Protocol interfaces to ensure proper dependency injection and
facilitates testing without requiring external dependencies.
"""

import os
import sys
import json
import tempfile
import pytest
import sqlite3
from unittest.mock import MagicMock, patch

from car_mcp.knowledge_graph_core_facade.db_handler import init_database # Updated import
from car_mcp.knowledge_graph_core_facade.kg_models_all import CacheProvider, ContextLogger # Updated import
from car_mcp.features.knowledge_graph_entities.services import EntityService # Import the service


@pytest.fixture
def temp_db_path():
    """Fixture providing a temporary database path for tests."""
    # Create a temporary file and get its path
    tmp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp_file.name
    # Close the file handle immediately so other processes (like SQLite) can access it
    tmp_file.close()

    try:
        yield db_path  # Provide the path to the test
    finally:
        # Clean up after tests
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except PermissionError:
                # On Windows, sometimes file handles are not released immediately.
                # Log a warning if deletion fails.
                # In a CI/CD environment, you might need a more robust cleanup.
                print(f"Warning: Could not delete temporary database {db_path} due to PermissionError.")


@pytest.fixture
def db_connection(temp_db_path):
    """Fixture providing a SQLite database connection for tests."""
    conn = init_database(temp_db_path)
    yield conn
    conn.close()


class MockCacheProvider:
    """
    Mock implementation of the CacheProvider interface for tests.
    
    This class implements the CacheProvider protocol and acts as a
    Redis-like in-memory cache for testing purposes, avoiding the
    need for a real Redis server.
    """
    
    def __init__(self):
        self.store = {}
        self.ttl_store = {}
        self.calls = {
            'get': [],
            'set': [],
            'delete': [],
            'exists': [],
            'keys': []
        }
    
    def get(self, key):
        """Get a value from the mock cache."""
        self.calls['get'].append(key)
        return self.store.get(key)
    
    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set a value in the mock cache."""
        self.calls['set'].append((key, value, ex, px, nx, xx))
        
        if nx and key in self.store:
            return False
        if xx and key not in self.store:
            return False
            
        self.store[key] = value
        
        if ex:
            self.ttl_store[key] = ex
        elif px:
            self.ttl_store[key] = px / 1000
            
        return True
    
    def exists(self, key):
        """Check if a key exists in the mock cache."""
        self.calls['exists'].append(key)
        return key in self.store
    
    def delete(self, *keys):
        """Delete key(s) from the mock cache."""
        self.calls['delete'].append(keys)
        count = 0
        for key in keys:
            if isinstance(key, str) and '*' in key:
                # Simple pattern matching for keys
                pattern = key.replace('*', '')
                matching_keys = [k for k in list(self.store.keys()) if pattern in k]
                for k in matching_keys:
                    if k in self.store:
                        del self.store[k]
                        if k in self.ttl_store:
                            del self.ttl_store[k]
                        count += 1
            elif key in self.store:
                del self.store[key]
                if key in self.ttl_store:
                    del self.ttl_store[key]  # Fixed: use key instead of k
                count += 1
        return count
    
    def keys(self, pattern=None):
        """Get keys matching a pattern."""
        self.calls['keys'].append(pattern)
        if pattern and '*' in pattern:
            # Simple pattern matching for keys
            pattern_part = pattern.replace('*', '')
            return [k for k in self.store.keys() if pattern_part in k]
        return list(self.store.keys())


class MockContextLogger:
    """
    Mock implementation of the ContextLogger protocol for tests.
    
    This class implements the ContextLogger protocol and records
    all event logging requests for later assertion in tests.
    """
    
    def __init__(self):
        self.events = []
    
    def log_event(self, event_name, data=None):
        """Log an event with optional data."""
        self.events.append((event_name, data))
        return None


@pytest.fixture
def mock_redis_client():
    """
    Fixture providing a mock Redis client for tests.
    
    This fixture returns an instance of MockCacheProvider wrapped
    in a MagicMock for compatibility with existing tests.
    """
    # Create a MockCacheProvider instance
    cache_provider = MockCacheProvider()
    
    # Create a MagicMock for compatibility with existing tests
    redis_mock = MagicMock()
    
    # Map the MagicMock methods to the MockCacheProvider methods
    redis_mock.get.side_effect = cache_provider.get
    redis_mock.set.side_effect = cache_provider.set
    redis_mock.exists.side_effect = cache_provider.exists
    redis_mock.delete.side_effect = cache_provider.delete
    redis_mock.keys.side_effect = cache_provider.keys
    
    # Store the cache provider for access to call history
    redis_mock._cache_provider = cache_provider
    
    return redis_mock


@pytest.fixture
def mock_embedding_function():
    """
    Fixture providing a mock embedding function for tests.
    
    This embedding function generates deterministic embeddings
    based on the input text, allowing for consistent test results.
    """
    def generate_mock_embedding(text):
        """Generate a deterministic mock embedding based on the text."""
        import hashlib
        hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
        # Generate a 10-dimensional embedding for simplicity
        return [(hash_val % 1000) / 1000.0 + i * 0.1 for i in range(10)]
    
    # Track calls for test assertions
    calls = []
    
    def tracked_function(text):
        calls.append(text)
        return generate_mock_embedding(text)
    
    # Store call history on the function
    tracked_function.calls = calls
    
    return tracked_function


@pytest.fixture
def mock_context_logger():
    """
    Fixture providing a mock context logger for tests.
    
    This fixture returns an instance of MockContextLogger that
    implements the ContextLogger protocol.
    """
    return MockContextLogger()


@pytest.fixture(autouse=True)
def mock_fastmcp():
    """
    Fixture providing mocks for FastMCP dependencies.
    
    This fixture creates a patch for the FastMCP module, allowing tests
    to run without having FastMCP installed. When applied with autouse=True,
    it automatically mocks out FastMCP for all tests.
    """
    # Create a mock for the FastMCP module with common API elements
    fastmcp_mock = MagicMock()
    
    # Define the classes and functions that might be imported
    fastmcp_mock.Tool = MagicMock()
    fastmcp_mock.Server = MagicMock()
    fastmcp_mock.Resource = MagicMock()
    fastmcp_mock.register_tools = MagicMock()
    fastmcp_mock.Context = MagicMock()
    
    # Additional utility functions
    fastmcp_mock.get_session = MagicMock(return_value=MagicMock())
    fastmcp_mock.create_server = MagicMock(return_value=MagicMock())
    
    # Create a patch for "fastmcp" imports in sys.modules
    with patch.dict('sys.modules', {'fastmcp': fastmcp_mock}):
        yield fastmcp_mock


@pytest.fixture
def enhanced_mock_redis_client():
    """
    Fixture providing an enhanced mock Redis client for tests.
    This version could potentially offer more detailed call tracking or
    specific behaviors if needed, but for now, it's similar to mock_redis_client.
    """
    cache_provider = MockCacheProvider()
    redis_mock = MagicMock()
    
    redis_mock.get.side_effect = cache_provider.get
    redis_mock.set.side_effect = cache_provider.set
    redis_mock.exists.side_effect = cache_provider.exists
    redis_mock.delete.side_effect = cache_provider.delete
    redis_mock.keys.side_effect = cache_provider.keys
    
    # Store the cache provider for access to call history and internal store
    redis_mock._cache_provider = cache_provider
    return redis_mock


@pytest.fixture
def entity_service_factory(db_connection, temp_db_path, mock_redis_client, mock_context_logger, mock_embedding_function):
    """Factory fixture to create instances of EntityService for tests."""
    def _create_service(cache_ttl=3600, lock=None):
        # Ensure db_connection is fresh for each service instance if tests modify db
        # For simplicity here, we use the session-scoped db_connection.
        # If tests require truly isolated dbs per service, db_connection might need to be function-scoped
        # or the factory would need to handle db setup/teardown.
        service = EntityService(
            conn=db_connection,
            db_path=temp_db_path,
            redis_client=mock_redis_client,
            context_logger=mock_context_logger,
            embedding_function=mock_embedding_function,
            cache_ttl=cache_ttl,
            lock=lock # Pass a shared lock if needed for concurrent test scenarios
        )
        return service
    return _create_service

@pytest.fixture
def entity_service(entity_service_factory):
    """Provides a standard EntityService instance with a clean temporary database."""
    return entity_service_factory()

@pytest.fixture
def in_memory_entity_service(entity_service_factory):
    """
    Provides an EntityService instance, typically for tests that might perform
    more extensive operations or where the 'in-memory' aspect implies a fresh,
    isolated environment (achieved via temp_db_path).
    """
    # For now, this is the same as entity_service. If true SQLite :memory:
    # is needed, temp_db_path and db_connection would need adjustment.
    return entity_service_factory()

@pytest.fixture
def populated_entity_service(entity_service, sample_entity_data, sample_relation_data, sample_observation_data):
    """
    Provides an EntityService instance pre-populated with sample data.
    Note: This fixture currently doesn't populate relations or observations as
    EntityService only handles entities. This would be expanded if EntityService
    also managed relations/observations or if we had separate services for those.
    """
    # Create a sample entity
    entity_id = entity_service.create_entity(
        name=sample_entity_data["name"],
        entity_type=sample_entity_data["entity_type"],
        properties=sample_entity_data["properties"]
    )
    # Store the created ID for potential use in tests, though tests should retrieve it.
    # setattr(populated_entity_service, "sample_entity_id", entity_id) # Not ideal to modify fixture like this
    
    # To make it truly populated for tests that expect it, we might store the ID
    # in a way that tests can access it, or tests should create their own specific setup.
    # For now, it just ensures one entity exists.
    return entity_service


@pytest.fixture
def sample_entity_data():
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


@pytest.fixture
def sample_relation_data():
    """Fixture providing sample relation data for tests."""
    return {
        "relation_type": "calls",
        "confidence": 0.95,
        "properties": {
            "count": 3,
            "locations": [45, 67, 89]
        }
    }


@pytest.fixture
def sample_observation_data():
    """Fixture providing sample observation data for tests."""
    return {
        "observation": "This function implements the core algorithm for processing data.",
        "properties": {
            "source": "documentation",
            "confidence": 0.9
        }
    }