"""
Metadata extractors for the CAR MCP server's document processor.

This module provides functions for extracting language-specific metadata
from code content, such as imports, functions, classes, etc.
"""

import re
from typing import Dict, Any

def extract_python_metadata(code: str) -> Dict[str, Any]:
    """
    Extract metadata from Python code.
    
    Args:
        code: Python code content
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Detect imports
    import_pattern = re.compile(r'^(?:from\s+[\w.]+\s+)?import\s+[\w.,\s]+', re.MULTILINE)
    imports = import_pattern.findall(code)
    metadata["imports"] = len(imports)
    
    # Detect functions
    function_pattern = re.compile(r'^def\s+(\w+)\s*\(', re.MULTILINE)
    functions = function_pattern.findall(code)
    metadata["functions"] = len(functions)
    
    # Detect classes
    class_pattern = re.compile(r'^class\s+(\w+)\s*(?:\([^)]*\))?:', re.MULTILINE)
    classes = class_pattern.findall(code)
    metadata["classes"] = len(classes)
    
    return metadata

def extract_js_ts_metadata(code: str) -> Dict[str, Any]:
    """
    Extract metadata from JavaScript/TypeScript code.
    
    Args:
        code: JavaScript/TypeScript code content
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Detect imports
    import_pattern = re.compile(r'^import\s+[\w{},\s]+\s+from\s+[\'"][\w./]+[\'"]', re.MULTILINE)
    imports = import_pattern.findall(code)
    metadata["imports"] = len(imports)
    
    # Detect functions
    function_pattern = re.compile(r'(?:function\s+(\w+)\s*\(|const\s+(\w+)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>)', re.MULTILINE)
    functions = function_pattern.findall(code)
    metadata["functions"] = len(functions)
    
    # Detect classes
    class_pattern = re.compile(r'class\s+(\w+)\s*(?:extends\s+\w+\s*)?{', re.MULTILINE)
    classes = class_pattern.findall(code)
    metadata["classes"] = len(classes)
    
    return metadata

def extract_java_metadata(code: str) -> Dict[str, Any]:
    """
    Extract metadata from Java code.
    
    Args:
        code: Java code content
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Detect imports
    import_pattern = re.compile(r'^import\s+[\w.]+;', re.MULTILINE)
    imports = import_pattern.findall(code)
    metadata["imports"] = len(imports)
    
    # Detect methods
    method_pattern = re.compile(r'(?:public|private|protected|static|\s) +[\w<>\[\]]+\s+(\w+) *\([^\)]*\) *(?:throws[\w\s,]+)? *\{', re.MULTILINE)
    methods = method_pattern.findall(code)
    metadata["methods"] = len(methods)
    
    # Detect classes
    class_pattern = re.compile(r'class\s+(\w+)\s*(?:extends\s+\w+\s*)?(?:implements\s+[\w,\s]+\s*)?{', re.MULTILINE)
    classes = class_pattern.findall(code)
    metadata["classes"] = len(classes)
    
    return metadata

def extract_go_metadata(code: str) -> Dict[str, Any]:
    """
    Extract metadata from Go code.
    
    Args:
        code: Go code content
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Detect imports
    import_pattern = re.compile(r'import\s+\(\s*(?:"[\w./]+"\s*)+\)', re.MULTILINE | re.DOTALL)
    imports = import_pattern.findall(code)
    metadata["imports"] = len(imports)
    
    # Detect functions
    function_pattern = re.compile(r'func\s+(?:\(\w+\s+\*?\w+\)\s+)?(\w+)\s*\(', re.MULTILINE)
    functions = function_pattern.findall(code)
    metadata["functions"] = len(functions)
    
    # Detect structs
    struct_pattern = re.compile(r'type\s+(\w+)\s+struct\s*{', re.MULTILINE)
    structs = struct_pattern.findall(code)
    metadata["structs"] = len(structs)
    
    return metadata

def extract_c_family_metadata(code: str) -> Dict[str, Any]:
    """
    Extract metadata from C-family languages (C, C++, C#).
    
    Args:
        code: C-family code content
        
    Returns:
        Dictionary of extracted metadata
    """
    metadata = {}
    
    # Detect includes/using
    include_pattern = re.compile(r'^(?:#include\s+[<"][\w./]+[">]|using\s+[\w.]+;)', re.MULTILINE)
    includes = include_pattern.findall(code)
    metadata["includes"] = len(includes)
    
    # Detect functions
    function_pattern = re.compile(r'(?:public|private|protected|static|\s) +[\w<>\[\]]+\s+(\w+) *\([^\)]*\) *(?:const)? *\{', re.MULTILINE)
    functions = function_pattern.findall(code)
    metadata["functions"] = len(functions)
    
    # Detect classes
    class_pattern = re.compile(r'class\s+(\w+)\s*(?::\s*(?:public|private|protected)\s+\w+\s*)?(?:,\s*(?:public|private|protected)\s+\w+\s*)*\{', re.MULTILINE)
    classes = class_pattern.findall(code)
    metadata["classes"] = len(classes)
    
    return metadata