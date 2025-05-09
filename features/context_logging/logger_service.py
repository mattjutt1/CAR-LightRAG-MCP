"""
Main context logger implementation for the CAR MCP server.

This module implements the ContextLogger class for tracking and visualizing
interactions with the CAR MCP server using Jupyter notebooks.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from nbformat import v4 as nbf
except ImportError:
    raise ImportError("nbformat is required. Please install it using: pip install nbformat")

from ...core.exceptions import ContextLoggerError # Assuming ContextLoggerError is moved to core.exceptions
from .visualization import generate_visualization_code

# Configure logging
logger = logging.getLogger("car_mcp.features.context_logging.logger_service")

class ContextLogger:
    """
    Context logger for tracking and visualizing CAR MCP server interactions.
    
    Uses Jupyter notebooks to store logs with rich visualization capabilities.
    """
    
    def __init__(self, log_dir: str):
        """
        Initialize the context logger.
        
        Args:
            log_dir: Directory to store log notebooks
        """
        self.log_dir = log_dir
        
        # Create log directory if it doesn't exist
        try:
            os.makedirs(log_dir, exist_ok=True)
            logger.info(f"Context logger initialized with log directory: {log_dir}")
        except Exception as e:
            error_msg = f"Failed to create log directory: {str(e)}"
            logger.error(error_msg)
            raise ContextLoggerError(error_msg)
    
    def create_log_notebook(self, name: str) -> str:
        """
        Create a new log notebook.
        
        Args:
            name: Name for the log notebook
            
        Returns:
            Path to the created notebook
        """
        try:
            # Create a new notebook
            notebook = nbf.new_notebook()
            
            # Add header cell with metadata
            header_cell = nbf.new_markdown_cell(
                f"# CAR MCP Server Log: {name}\n\n"
                f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"This notebook contains logs for CAR MCP server interactions."
            )
            
            # Add metadata for better display in Jupyter
            notebook.metadata = {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "codemirror_mode": {
                        "name": "ipython",
                        "version": 3
                    },
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.8.0"
                }
            }
            
            # Add cells to notebook
            notebook.cells = [header_cell]
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = name.replace(" ", "_").replace("/", "_").replace("\\", "_")
            filename = f"{safe_name}_{timestamp}.ipynb"
            filepath = os.path.join(self.log_dir, filename)
            
            # Write notebook to file
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(notebook, f)
            
            logger.info(f"Created log notebook: {filepath}")
            return filepath
            
        except Exception as e:
            error_msg = f"Failed to create log notebook: {str(e)}"
            logger.error(error_msg)
            raise ContextLoggerError(error_msg)
    
    def log_code_interaction(
        self, 
        notebook_path: str, 
        query: str, 
        results: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a code interaction to a notebook.
        
        Args:
            notebook_path: Path to the notebook
            query: The query or interaction that triggered the results
            results: The results or response from the interaction
            metadata: Optional additional metadata about the interaction
        """
        try:
            # Load existing notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)
            
            # Add timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create markdown cell for query and metadata
            markdown_content = f"## Query ({timestamp})\n\n"
            markdown_content += f"```\n{query}\n```\n\n"
            
            if metadata:
                markdown_content += "### Metadata\n\n"
                markdown_content += "```json\n"
                markdown_content += json.dumps(metadata, indent=2)
                markdown_content += "\n```\n\n"
            
            query_cell = nbf.new_markdown_cell(markdown_content)
            
            # Create cells for results
            cells = [query_cell]
            
            for i, result in enumerate(results):
                # Add markdown cell for result
                result_markdown = f"### Result {i+1}\n\n"
                
                # Add metadata if available
                if "metadata" in result:
                    result_markdown += "**Metadata:**\n\n"
                    result_markdown += "```json\n"
                    result_markdown += json.dumps(result["metadata"], indent=2)
                    result_markdown += "\n```\n\n"
                
                # Add content with proper syntax highlighting
                language = result.get("metadata", {}).get("language", "text")
                content = result.get("content", "")
                
                result_markdown += f"**Content:**\n\n"
                result_markdown += f"```{language}\n{content}\n```\n\n"
                
                # Add distance or other metrics if available
                if "distance" in result:
                    result_markdown += f"**Similarity Score:** {1.0 - result['distance']:.4f}\n\n"
                
                result_cell = nbf.new_markdown_cell(result_markdown)
                cells.append(result_cell)
            
            # Add visualization code cell
            if results and len(results) > 1:
                vis_code = generate_visualization_code(results)
                vis_cell = nbf.new_code_cell(vis_code)
                cells.append(vis_cell)
            
            # Add separator cell
            separator_cell = nbf.new_markdown_cell("---")
            cells.append(separator_cell)
            
            # Append cells to notebook
            notebook["cells"].extend(cells)
            
            # Write updated notebook back to file
            with open(notebook_path, "w", encoding="utf-8") as f:
                json.dump(notebook, f)
            
            logger.info(f"Logged code interaction to notebook: {notebook_path}")
            
        except Exception as e:
            error_msg = f"Failed to log to notebook: {str(e)}"
            logger.error(error_msg)
            raise ContextLoggerError(error_msg)
    
    def log_memory_operation(
        self, 
        notebook_path: str, 
        operation: str, 
        documents: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a memory operation to a notebook.
        
        Args:
            notebook_path: Path to the notebook
            operation: The operation performed (e.g., "add", "delete")
            documents: The documents involved in the operation
            metadata: Optional additional metadata about the operation
        """
        try:
            # Load existing notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)
            
            # Add timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create markdown cell for operation and metadata
            markdown_content = f"## Memory Operation: {operation} ({timestamp})\n\n"
            
            if metadata:
                markdown_content += "### Metadata\n\n"
                markdown_content += "```json\n"
                markdown_content += json.dumps(metadata, indent=2)
                markdown_content += "\n```\n\n"
            
            operation_cell = nbf.new_markdown_cell(markdown_content)
            
            # Create cells for documents
            cells = [operation_cell]
            
            for i, document in enumerate(documents):
                # Add markdown cell for document
                doc_markdown = f"### Document {i+1}\n\n"
                
                # Add metadata if available
                if "metadata" in document:
                    doc_markdown += "**Metadata:**\n\n"
                    doc_markdown += "```json\n"
                    doc_markdown += json.dumps(document["metadata"], indent=2)
                    doc_markdown += "\n```\n\n"
                
                # Add content with proper syntax highlighting
                language = document.get("metadata", {}).get("language", "text")
                content = document.get("content", "")
                
                doc_markdown += f"**Content:**\n\n"
                doc_markdown += f"```{language}\n{content}\n```\n\n"
                
                doc_cell = nbf.new_markdown_cell(doc_markdown)
                cells.append(doc_cell)
            
            # Add separator cell
            separator_cell = nbf.new_markdown_cell("---")
            cells.append(separator_cell)
            
            # Append cells to notebook
            notebook["cells"].extend(cells)
            
            # Write updated notebook back to file
            with open(notebook_path, "w", encoding="utf-8") as f:
                json.dump(notebook, f)
            
            logger.info(f"Logged memory operation to notebook: {notebook_path}")
            
        except Exception as e:
            error_msg = f"Failed to log to notebook: {str(e)}"
            logger.error(error_msg)
            raise ContextLoggerError(error_msg)
    
    def list_notebooks(self) -> List[str]:
        """
        List all log notebooks in the log directory.
        
        Returns:
            List of notebook paths
        """
        try:
            notebooks = []
            for filename in os.listdir(self.log_dir):
                if filename.endswith(".ipynb"):
                    notebooks.append(os.path.join(self.log_dir, filename))
            return notebooks
        except Exception as e:
            error_msg = f"Failed to list notebooks: {str(e)}"
            logger.error(error_msg)
            raise ContextLoggerError(error_msg)