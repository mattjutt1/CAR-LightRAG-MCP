"""
Feature slice for Knowledge Graph Entity Management.

This package includes:
- `ops_entity_crud.py`: Low-level CRUD operations for entities.
- `entity_manager.py`: Business logic and coordination for entity management (if distinct from service).
- `services.py`: Defines `EntityService` as the primary API for this feature slice.
- `models.py` (optional): Entity-specific data models if not centralized.
"""

# Re-export the main service class for this feature slice
from .services import EntityService

__all__ = ['EntityService']