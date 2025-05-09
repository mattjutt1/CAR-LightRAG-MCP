"""
Feature slice for Knowledge Graph Maintenance operations.

This package includes:
- `ops_maintenance.py`: Low-level maintenance operations (clear, backup, stats).
- `maintenance_manager.py`: Business logic and coordination for maintenance tasks.
- `services.py`: Defines `MaintenanceService` as the primary API for this feature slice.
"""

# Re-export the main service class for this feature slice
# from .services import MaintenanceService

# __all__ = ['MaintenanceService']