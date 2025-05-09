"""
Core utilities sub-package.

This package contains generic, cross-cutting utility functions
and classes that can be used throughout the application.
"""

# Re-export specific utilities for convenience
from .file_utils import safe_read_file, is_binary_file
from .json_utils import serialize_properties, deserialize_properties

__all__ = [
    "safe_read_file",
    "is_binary_file",
    "serialize_properties",
    "deserialize_properties",
]