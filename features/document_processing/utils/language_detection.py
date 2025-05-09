"""
Language detection utilities for the CAR MCP server.

This module provides functions for detecting programming languages from file extensions and content.
"""

import re
from typing import Dict

# Regular expression for identifying language from file extension
FILE_EXT_PATTERN = re.compile(r'\.([a-zA-Z0-9]+)$')

# Common file extensions to language mappings
EXTENSION_TO_LANGUAGE = {
    'py': 'python',
    'js': 'javascript',
    'ts': 'typescript',
    'jsx': 'jsx',
    'tsx': 'tsx',
    'java': 'java',
    'c': 'c',
    'cpp': 'cpp',
    'cs': 'csharp',
    'go': 'go',
    'rb': 'ruby',
    'php': 'php',
    'html': 'html',
    'css': 'css',
    'sql': 'sql',
    'sh': 'bash',
    'md': 'markdown',
    'json': 'json',
    'yaml': 'yaml',
    'yml': 'yaml',
    'rs': 'rust',
    'swift': 'swift',
    'kt': 'kotlin',
    'scala': 'scala',
}

def detect_language_from_file(file_path: str) -> str:
    """
    Detect programming language from file extension.
    
    Args:
        file_path: Path to the file
        
    Returns:
        The detected language or 'text' if unknown
    """
    match = FILE_EXT_PATTERN.search(file_path)
    if match:
        extension = match.group(1).lower()
        return EXTENSION_TO_LANGUAGE.get(extension, 'text')
    return 'text'

def detect_language_from_content(content: str) -> str:
    """
    Attempt to detect programming language from content heuristics.
    This is a fallback when file extension is not available.
    
    Args:
        content: Code content
        
    Returns:
        The detected language or 'text' if unknown
    """
    # This is a simplified heuristic - could be expanded with more sophisticated detection
    if 'def ' in content and ('self' in content or 'import ' in content):
        return 'python'
    elif 'function ' in content and ('{' in content and '}' in content):
        return 'javascript'
    elif 'class ' in content and ('extends ' in content or 'implements ' in content):
        if 'public static void main' in content:
            return 'java'
        elif '<' in content and '/>' in content:
            return 'typescript'
        else:
            return 'text'
    elif '<html' in content.lower() or '</html>' in content.lower():
        return 'html'
    elif '@media' in content or '{' in content and '}' in content and (':' in content and ';' in content):
        return 'css'
    elif 'SELECT ' in content.upper() or 'INSERT INTO' in content.upper():
        return 'sql'
    return 'text'