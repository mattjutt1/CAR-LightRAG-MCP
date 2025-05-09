"""
Core JSON utility functions.
"""
import json
from typing import Dict, Any

def serialize_properties(properties: Dict[str, Any]) -> str:
    """
    Serialize a properties dictionary to a JSON string for storage.
    
    Args:
        properties: Dictionary of properties
        
    Returns:
        JSON string representation of properties
    """
    return json.dumps(properties)

def deserialize_properties(properties_str: str) -> Dict[str, Any]:
    """
    Deserialize a JSON string to a properties dictionary.
    
    Args:
        properties_str: JSON string of properties
        
    Returns:
        Dictionary of properties
    """
    if not properties_str:
        return {}
    return json.loads(properties_str)