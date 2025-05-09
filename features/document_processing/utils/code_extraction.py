"""
Code extraction utilities for the CAR MCP server.

This module provides functions for extracting code blocks from markdown and other text formats.
"""

import re
from typing import Dict, List

# Regular expressions for extracting code blocks from markdown
MD_CODE_BLOCK_PATTERN = re.compile(r'```(?:(\w+))?\s*\n(.*?)\n```', re.DOTALL)
INLINE_CODE_PATTERN = re.compile(r'`([^`]+)`')

def extract_code_blocks(text: str) -> List[Dict[str, str]]:
    """
    Extract code blocks from markdown-formatted text.
    
    Args:
        text: Text containing markdown code blocks
        
    Returns:
        List of dictionaries containing code blocks with their language
    """
    code_blocks = []
    
    # Extract fenced code blocks
    for match in MD_CODE_BLOCK_PATTERN.finditer(text):
        language = match.group(1) or 'text'
        code = match.group(2)
        code_blocks.append({
            'language': language.lower(),
            'code': code,
            'type': 'block'
        })
    
    # Extract inline code (disabled by default as they're often not complete code snippets)
    # for match in INLINE_CODE_PATTERN.finditer(text):
    #     code = match.group(1)
    #     code_blocks.append({
    #         'language': 'text',
    #         'code': code,
    #         'type': 'inline'
    #     })
    
    return code_blocks