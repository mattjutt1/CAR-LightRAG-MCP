"""
Core protocols (interfaces) for Dependency Injection.

This module defines Abstract Base Classes (ABCs) or typing.Protocols
for services and components that will be injected throughout the application.
This promotes loose coupling and enhances testability.
"""
from typing import Protocol

# Example Protocol (can be expanded later)
# class SomeServiceProtocol(Protocol):
#     def perform_action(self, data: str) -> bool:
#         ...