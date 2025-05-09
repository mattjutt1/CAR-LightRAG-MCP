# Knowledge Graph Testing Framework

This document provides a comprehensive guide to the testing framework for the Knowledge Graph component of the CAR/LightRAG MCP server.

## Overview

The testing framework is designed to ensure the reliability and correctness of the Knowledge Graph component. It includes:

- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test interactions between components
- **In-memory testing**: Fast tests using SQLite in-memory databases
- **Redis mocking**: Comprehensive Redis cache testing
- **Fixtures**: Reusable test setup and teardown logic
- **Parameterized tests**: Testing multiple scenarios efficiently

## Directory Structure

```
car_mcp/tests/
├── README.md                  # This documentation
├── conftest.py                # Global test fixtures
├── run_tests.py               # Test runner script
└── knowledge_graph/           # Knowledge Graph specific tests
    ├── conftest.py            # KG-specific fixtures
    ├── unit/                  # Unit tests
    │   ├── test_entity_operations.py
    │   ├── test_relation_operations.py
    │   ├── test_observation_operations.py
    │   ├── test_search_operations.py
    │   └── test_maintenance_operations.py
    └── integration/           # Integration tests
        ├── test_knowledge_graph.py
        ├── test_redis_integration.py
        └── test_database_persistence.py
```

## Running Tests

The `run_tests.py` script provides a flexible way to run tests with various options:

### Basic Usage

```bash
# Run all Knowledge Graph tests
python -m car_mcp.tests.run_tests

# Run only unit tests
python -m car_mcp.tests.run_tests --unit

# Run only integration tests
python -m car_mcp.tests.run_tests --integration

# Run specific test categories
python -m car_mcp.tests.run_tests --entity     # Entity operations
python -m car_mcp.tests.run_tests --relation   # Relation operations
python -m car_mcp.tests.run_tests --observation # Observation operations
python -m car_mcp.tests.run_tests --search     # Search operations
python -m car_mcp.tests.run_tests --maintenance # Maintenance operations
python -m car_mcp.tests.run_tests --redis      # Redis integration
python -m car_mcp.tests.run_tests --database   # Database persistence
```

### Output and Reporting Options

```bash
# Increase verbosity
python -m car_mcp.tests.run_tests -v      # Verbose
python -m car_mcp.tests.run_tests -vv     # Very verbose

# Generate coverage reports
python -m car_mcp.tests.run_tests --cov           # Terminal coverage report
python -m car_mcp.tests.run_tests --cov-html     # HTML coverage report
python -m car_mcp.tests.run_tests --cov-xml      # XML coverage report for CI

# Generate JUnit XML report for CI tools
python -m car_mcp.tests.run_tests --junit-xml

# Specify report directory
python -m car_mcp.tests.run_tests --cov-html --report-dir=./my-reports
```

### Test Execution Options

```bash
# Stop after first failure
python -m car_mcp.tests.run_tests --xvs

# Run tests with specific markers
python -m car_mcp.tests.run_tests --markers slow integration

# Run tests matching keyword expression
python -m car_mcp.tests.run_tests --keyword "entity and not delete"

# Run tests in parallel
python -m car_mcp.tests.run_tests --parallel
python -m car_mcp.tests.run_tests --parallel --max-workers=4

# Run tests multiple times (detect flaky tests)
python -m car_mcp.tests.run_tests --repeat=3

# Don't use pytest cache
python -m car_mcp.tests.run_tests --no-cache
```

### Passing Additional Arguments to pytest

You can pass additional arguments directly to pytest:

```bash
python -m car_mcp.tests.run_tests --pytest-args -xvs --maxfail=1
```

## Fixtures

The testing framework provides various fixtures to simplify test setup and teardown:

### Basic Database Fixtures

- **`temp_db_path`**: Provides a temporary database file path for tests
- **`db_connection`**: Provides a SQLite database connection for tests
- **`in_memory_db_connection`**: Provides an in-memory SQLite database connection for faster tests

### Redis Fixtures

- **`mock_redis_client`**: Provides a basic mock Redis client
- **`enhanced_mock_redis_client`**: Provides an enhanced mock Redis client with more functionality

### Knowledge Graph Fixtures

- **`knowledge_graph`**: Provides a configured Knowledge Graph instance
- **`in_memory_knowledge_graph`**: Provides a Knowledge Graph instance with an in-memory database
- **`populated_knowledge_graph`**: Provides a Knowledge Graph with sample data
- **`populated_in_memory_knowledge_graph`**: Provides an in-memory Knowledge Graph with sample data

### Data Fixtures

- **`sample_entity_data`**: Provides sample entity data
- **`sample_relation_data`**: Provides sample relation data
- **`sample_observation_data`**: Provides sample observation data
- **`deterministic_embedding_function`**: Provides a consistent embedding function
- **`mock_embedding_function`**: Provides a simple mock embedding function
- **`mock_context_logger`**: Provides a mock context logger
- **`backup_dir`**: Provides a temporary directory for backup/restore tests

## Writing Tests

### Unit Tests

Unit tests should focus on testing individual operations in isolation. Use the in-memory database fixtures for faster tests.

Example:

```python
def test_create_entity_basic(self, in_memory_knowledge_graph, sample_entity_data):
    """Test creating a basic entity with minimal attributes."""
    entity_id = in_memory_knowledge_graph.create_entity(
        name=sample_entity_data["name"],
        entity_type=sample_entity_data["entity_type"]
    )
    
    # Verify entity was created
    entity = in_memory_knowledge_graph.get_entity(entity_id)
    assert entity is not None
    assert entity.name == sample_entity_data["name"]
```

### Integration Tests

Integration tests should focus on interactions between components. Test Redis integration, database persistence, and full Knowledge Graph functionality.

Example:

```python
def test_entity_cache_operations(self, temp_db_path, enhanced_mock_redis_client):
    """Test entity caching operations including hits, misses, and invalidation."""
    # Create knowledge graph with Redis cache
    kg = KnowledgeGraph(
        db_path=temp_db_path,
        redis_client=enhanced_mock_redis_client
    )
    
    # Test cache operations
    entity_id = kg.create_entity(name="CachedEntity", entity_type="test")
    
    # Verify cache is populated
    enhanced_mock_redis_client.set.assert_any_call(
        f"entity:{entity_id}",
        pytest.approx(any),
        ex=mock_redis_client.set.call_args_list[0][1].get('ex', 3600)
    )
```

### Best Practices

1. **Use Fixtures**: Leverage the provided fixtures to simplify test setup
2. **Prefer In-Memory Databases**: Use in-memory databases for faster tests
3. **Test Edge Cases**: Include tests for error handling and edge cases
4. **Use Parameterized Tests**: Use `@pytest.mark.parametrize` for testing multiple scenarios
5. **Mock External Dependencies**: Use `patch` and `MagicMock` for external dependencies
6. **Use Context Managers**: Use `with` statements for resource cleanup
7. **Check Cache Invalidation**: Verify cache is invalidated on updates and deletes
8. **Verify Foreign Key Constraints**: Test that database constraints work as expected
9. **Test Transactions**: Verify that transactions maintain data integrity
10. **Test Concurrent Access**: Verify that concurrent operations work correctly

## Tips for Fast Tests

1. **Use In-Memory Databases**: In-memory SQLite databases are much faster than file-based ones
2. **Mock Redis**: Use the provided Redis mock fixtures instead of a real Redis instance
3. **Run Tests in Parallel**: Use the `--parallel` option to run tests in parallel
4. **Focus Testing**: Run only the tests you need with the appropriate command-line options
5. **Cache Test Results**: Pytest caches test results; use `--no-cache` only when needed

## Continuous Integration

The testing framework is designed to work well with CI systems:

1. **Coverage Reports**: Generate coverage reports using `--cov-xml`
2. **JUnit XML Reports**: Generate JUnit XML reports using `--junit-xml`
3. **Parallel Testing**: Speed up CI runs with `--parallel`
4. **Flaky Test Detection**: Use `--repeat` to detect flaky tests

## Troubleshooting

If you encounter issues with the tests:

1. **Run with Increased Verbosity**: Use `-vv` for more detailed output
2. **Check Database Connections**: Ensure SQLite is working correctly
3. **Verify Redis Mocking**: Ensure Redis mocks are configured correctly
4. **Check Test Isolation**: Make sure tests don't interfere with each other
5. **Look for Test Dependencies**: Ensure tests don't depend on the order of execution