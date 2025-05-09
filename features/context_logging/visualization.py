"""
Visualization utilities for the CAR MCP server's context logger.

This module provides functions for generating visualizations of search results
and other data in Jupyter notebooks.
"""

from typing import Dict, List, Any

def generate_visualization_code(results: List[Dict[str, Any]]) -> str:
    """
    Generate Python code for visualizing search results.
    
    Args:
        results: The search results to visualize
        
    Returns:
        Python code for visualization
    """
    # This generates Python code that will be executed in the notebook
    # to create visualizations of the search results
    
    code = """
import matplotlib.pyplot as plt
import numpy as np
from IPython.display import display, Markdown

# Extract similarity scores
scores = []
labels = []

"""
    
    # Add code to extract data from results
    code += "# Data from results\n"
    code += "for i, result in enumerate([\n"
    
    for result in results:
        metadata = result.get("metadata", {})
        distance = result.get("distance", 0)
        similarity = 1.0 - distance if distance is not None else 0.5
        
        filename = metadata.get("file_name", "") or metadata.get("file_path", "").split("/")[-1]
        if not filename:
            filename = f"Result {len(code.split('result.append('))+1}"
        
        code += f"    {{'similarity': {similarity}, 'label': '{filename}'}},\n"
    
    code += "]):\n"
    code += "    scores.append(result['similarity'])\n"
    code += "    labels.append(result['label'])\n\n"
    
    # Add visualization code
    code += """
# Create bar chart
plt.figure(figsize=(10, 6))
y_pos = np.arange(len(labels))
plt.barh(y_pos, scores, align='center', alpha=0.5)
plt.yticks(y_pos, labels)
plt.xlabel('Similarity Score')
plt.title('Search Results Similarity')

# Add score values at the end of each bar
for i, v in enumerate(scores):
    plt.text(v + 0.01, i, f"{v:.4f}", va='center')

plt.tight_layout()
plt.show()

# Display summary
display(Markdown("### Summary of Results"))
for i, (label, score) in enumerate(zip(labels, scores)):
    display(Markdown(f"- **{label}**: Similarity score {score:.4f}"))
"""
    
    return code