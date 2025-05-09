"""
Entity, Relation, and Observation model classes for the Knowledge Graph.

This module defines the core data models for the CAR/LightRAG knowledge graph:
- Entity: Represents a code element (function, class, file, etc.)
- Observation: Represents a piece of information about an entity
- Relation: Represents a relationship between two entities

This module also defines interfaces for services and dependencies used by the
Knowledge Graph component, enabling proper dependency injection and testing.
"""

import uuid
# import abc # F401 unused
from dataclasses import dataclass, field
from datetime import datetime
# Union, Callable, TypeVar removed (F401)
from typing import List, Dict, Any, Optional, Protocol, runtime_checkable


# Define interfaces for dependencies

@runtime_checkable
class CacheProvider(Protocol):
    """Interface for cache providers (like Redis)."""
    
    def get(self, key: str) -> Optional[str]:
        """Get a value from the cache by key."""
        ...
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Set a value in the cache with optional expiration."""
        ...
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys from the cache."""
        ...
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        ...


@runtime_checkable
class ContextLogger(Protocol):
    """Interface for context logging."""
    
    def log_event(self, event_name: str, data: Any = None) -> None:
        """Log an event with optional data."""
        ...


@runtime_checkable
class EmbeddingFunction(Protocol):
    """Interface for embedding generation functions."""
    
    def __call__(self, text: str) -> List[float]:
        """Generate an embedding vector for the given text."""
        ...


# Core model classes

@dataclass
class Entity:
    """
    An entity in the knowledge graph representing a code element.
    
    Entities can be functions, classes, files, modules, or other code elements.
    Each entity has a unique ID, a name, a type, and can have an embedding
    for semantic search.
    """
    name: str
    entity_type: str
    embedding: Optional[List[float]] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary representation for database storage."""
        return {
            "id": self.id,
            "name": self.name,
            "entity_type": self.entity_type,
            "embedding": self.embedding,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "properties": self.properties
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """Create an entity from a dictionary representation."""
        # Handle datetime conversion from string
        created_at_val = data["created_at"]
        updated_at_val = data["updated_at"]
        created_at = (datetime.fromisoformat(created_at_val)
                      if isinstance(created_at_val, str) else created_at_val)
        updated_at = (datetime.fromisoformat(updated_at_val)
                      if isinstance(updated_at_val, str) else updated_at_val)

        return cls(
            id=data["id"],
            name=data["name"],
            entity_type=data["entity_type"],
            embedding=data.get("embedding"),
            created_at=created_at,
            updated_at=updated_at,
            properties=data.get("properties", {})
        )
    
    def update(self, **kwargs) -> None:
        """Update entity attributes and set updated_at to current time."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        self.updated_at = datetime.now()


@dataclass
class Observation:
    """
    An observation about an entity in the knowledge graph.
    
    Observations represent contextual information about entities,
    such as documentation, usage patterns, or code characteristics.
    """
    entity_id: str
    observation: str
    embedding: Optional[List[float]] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert observation to dictionary representation for database storage."""
        return {
            "id": self.id,
            "entity_id": self.entity_id,
            "observation": self.observation,
            "embedding": self.embedding,
            "created_at": self.created_at.isoformat(),
            "properties": self.properties
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Observation':
        """Create an observation from a dictionary representation."""
        # Handle datetime conversion from string
        created_at_val = data["created_at"]
        created_at = (datetime.fromisoformat(created_at_val)
                      if isinstance(created_at_val, str) else created_at_val)

        return cls(
            id=data["id"],
            entity_id=data["entity_id"],
            observation=data["observation"],
            embedding=data.get("embedding"),
            created_at=created_at,
            properties=data.get("properties", {})
        )


@dataclass
class Relation:
    """
    A relation between two entities in the knowledge graph.
    
    Relations represent directed connections between entities, such as
    "imports", "calls", "inherits_from", etc., with an associated confidence.
    """
    from_entity_id: str
    to_entity_id: str
    relation_type: str
    confidence: float = 1.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.now)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert relation to dictionary representation for database storage."""
        return {
            "id": self.id,
            "from_entity_id": self.from_entity_id,
            "to_entity_id": self.to_entity_id,
            "relation_type": self.relation_type,
            "confidence": self.confidence,
            "created_at": self.created_at.isoformat(),
            "properties": self.properties
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relation':
        """Create a relation from a dictionary representation."""
        # Handle datetime conversion from string
        created_at_val = data["created_at"]
        created_at = (datetime.fromisoformat(created_at_val)
                      if isinstance(created_at_val, str) else created_at_val)

        return cls(
            id=data["id"],
            from_entity_id=data["from_entity_id"],
            to_entity_id=data["to_entity_id"],
            relation_type=data["relation_type"],
            confidence=data["confidence"],
            created_at=created_at,
            properties=data.get("properties", {})
        )

