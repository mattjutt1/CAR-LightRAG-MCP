"""
Database handler for the Knowledge Graph.

This module is responsible for SQLite database connection management,
schema definition, creation, and other low-level database operations.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional, Dict # Removed Any, List, Tuple

logger = logging.getLogger(__name__)

# SQL statements for creating the schema
CREATE_ENTITIES_TABLE = """
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    embedding TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    properties TEXT
)
"""

CREATE_OBSERVATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS observations (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    observation TEXT NOT NULL,
    embedding TEXT,
    created_at TIMESTAMP NOT NULL,
    properties TEXT,
    FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE
)
"""

CREATE_RELATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS relations (
    id TEXT PRIMARY KEY,
    from_entity_id TEXT NOT NULL,
    to_entity_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    confidence REAL DEFAULT 1.0,
    created_at TIMESTAMP NOT NULL,
    properties TEXT,
    FOREIGN KEY (from_entity_id) REFERENCES entities (id) ON DELETE CASCADE,
    FOREIGN KEY (to_entity_id) REFERENCES entities (id) ON DELETE CASCADE
)
"""

# SQL statements for creating indexes
CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_entity_name ON entities (name)",
    "CREATE INDEX IF NOT EXISTS idx_entity_type ON entities (entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_observation_entity "
    "ON observations (entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relation_from "
    "ON relations (from_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relation_to ON relations (to_entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_relation_type ON relations (relation_type)"
]


def init_database(db_path: str) -> sqlite3.Connection: # E302
    """
    Initialize the SQLite database with the Knowledge Graph schema.
    
    Args:
        db_path: Path to the SQLite database file
    Returns:
        A connection to the initialized database
    """
    # Ensure the directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn: Optional[sqlite3.Connection] = None
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")

        # Create tables
        conn.execute(CREATE_ENTITIES_TABLE)
        conn.execute(CREATE_OBSERVATIONS_TABLE)
        conn.execute(CREATE_RELATIONS_TABLE)

        # Create indexes
        for index_sql in CREATE_INDEXES:
            conn.execute(index_sql)

        # Commit changes
        conn.commit()

        logger.info(
            f"Successfully initialized Knowledge Graph database at {db_path}" # E501
        )
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error initializing Knowledge Graph database: {e}")
        if conn:
            conn.close()
        raise


def get_connection(db_path: str) -> sqlite3.Connection: # E302
    """
    Get a connection to the Knowledge Graph database.
    
    Args:
        db_path: Path to the SQLite database file
    Returns:
        A connection to the database
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")

        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to Knowledge Graph database: {e}")
        raise


def check_table_exists(conn: sqlite3.Connection, table_name: str) -> bool: # E302
    """
    Check if a table exists in the database.
    
    Args:
        conn: Database connection
        table_name: Name of the table to check
    Returns:
        True if the table exists, False otherwise
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_database_size(db_path: str) -> int: # E302
    """
    Get the size of the database file in bytes.
    
    Args:
        db_path: Path to the SQLite database file
    Returns:
        Size of the database file in bytes
    """
    import os  # Moved import inside function as it's specific  # E261
    if os.path.exists(db_path):
        return os.path.getsize(db_path)
    return 0


def vacuum_database(conn: sqlite3.Connection) -> None: # E302
    """
    Vacuum the database to reclaim unused space.
    
    Args:
        conn: Database connection
    """
    try:
        conn.execute("VACUUM")
        conn.commit()
        logger.info("Database vacuumed successfully")
    except sqlite3.Error as e:
        logger.error(f"Error vacuuming database: {e}")
        raise


def get_table_counts(conn: sqlite3.Connection) -> Dict[str, int]: # E302
    """
    Get the row counts for each table in the database.
    
    Args:
        conn: Database connection
    Returns:
        Dictionary mapping table names to row counts
    """
    counts = {}
    for table in ['entities', 'observations', 'relations']:
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        except sqlite3.Error:
            counts[table] = -1  # Error flag
    return counts

# Note: serialize_properties and deserialize_properties from the original
# schema.py will be moved to a different utility file,
# e.g., core/utils/json_utils.py

