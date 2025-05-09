"""
Code Augmented Reasoning (CAR) MCP Server

A Model Context Protocol (MCP) server implementation for code understanding, 
storage, and retrieval that enhances Claude's code interactions.

Designed to work with Anthropic's Claude desktop app and Claude Code.
"""

__version__ = "0.1.0"

# from .server import CARServer  # Temporarily commented out to resolve test loading
# from .memory import MemoryClient # Temporarily commented out
# from .tools import register_tools # Temporarily commented out
from car_mcp.core.exceptions import CARException, DocumentProcessingError, MemoryError # Corrected import path