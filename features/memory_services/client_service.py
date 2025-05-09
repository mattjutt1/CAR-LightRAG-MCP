"""
Memory client implementation for the CAR MCP server.

This module implements the MemoryClient class which provides vector storage
and retrieval capabilities using ChromaDB.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union

try:
    import chromadb
    from chromadb.config import Settings
except ImportError:
    raise ImportError("ChromaDB is required. Please install it using: pip install chromadb")

from ...core.exceptions import MemoryError, VectorStoreError, SearchError # Assuming these are moved to core

# Configure logging
logger = logging.getLogger("car_mcp.features.memory_services.client_service")

class MemoryClient:
    """
    Memory client for storing and retrieving code knowledge using vector embeddings.
    
    Uses ChromaDB as the underlying vector storage system.
    """
    
    def __init__(
        self, 
        db_path: str,
        collection_name: str = "code_knowledge",
        embedding_function = None
    ):
        """
        Initialize the memory client.
        
        Args:
            db_path: Path to store the ChromaDB database
            collection_name: Name of the collection to store code embeddings
            embedding_function: Optional custom embedding function
        """
        self.db_path = db_path
        self.collection_name = collection_name
        
        # Create the directory if it doesn't exist
        os.makedirs(db_path, exist_ok=True)
        
        try:
            # Initialize ChromaDB client
            self.client = chromadb.PersistentClient(
                path=db_path,
                settings=Settings(
                    anonymized_telemetry=False
                )
            )
            
            # Get or create collection
            self.collection = self.client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_function
            )
            
            logger.info(f"Initialized ChromaDB at {db_path} with collection '{collection_name}'")
            
        except Exception as e:
            error_msg = f"Failed to initialize ChromaDB: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)
    
    def add_document(
        self, 
        document_id: str, 
        content: str, 
        metadata: Dict[str, Any],
        embeddings: Optional[List[float]] = None
    ) -> str:
        """
        Add a document to the vector store.
        
        Args:
            document_id: Unique identifier for the document
            content: The document content to embed
            metadata: Additional metadata about the document
            embeddings: Optional pre-computed embeddings
            
        Returns:
            The document ID
        """
        try:
            # Ensure metadata is JSON serializable
            serializable_metadata = {
                k: v if isinstance(v, (str, int, float, bool, type(None))) else str(v)
                for k, v in metadata.items()
            }
            
            # Add to collection
            self.collection.add(
                ids=[document_id],
                documents=[content],
                metadatas=[serializable_metadata],
                embeddings=[embeddings] if embeddings else None
            )
            
            logger.info(f"Added document with ID {document_id} to vector store")
            return document_id
            
        except Exception as e:
            error_msg = f"Failed to add document to vector store: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)
    
    def add_documents(
        self, 
        document_ids: List[str], 
        contents: List[str], 
        metadatas: List[Dict[str, Any]],
        embeddings: Optional[List[List[float]]] = None
    ) -> List[str]:
        """
        Add multiple documents to the vector store.
        
        Args:
            document_ids: List of unique identifiers for the documents
            contents: List of document contents to embed
            metadatas: List of additional metadata about the documents
            embeddings: Optional pre-computed embeddings
            
        Returns:
            The list of document IDs
        """
        if len(document_ids) != len(contents) or len(document_ids) != len(metadatas):
            raise ValueError("document_ids, contents, and metadatas must have the same length")
            
        try:
            # Ensure metadata is JSON serializable
            serializable_metadatas = [
                {
                    k: v if isinstance(v, (str, int, float, bool, type(None))) else str(v)
                    for k, v in metadata.items()
                }
                for metadata in metadatas
            ]
            
            # Add to collection
            self.collection.add(
                ids=document_ids,
                documents=contents,
                metadatas=serializable_metadatas,
                embeddings=embeddings
            )
            
            logger.info(f"Added {len(document_ids)} documents to vector store")
            return document_ids
            
        except Exception as e:
            error_msg = f"Failed to add documents to vector store: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)
    
    def search(
        self, 
        query: str, 
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        embedding: Optional[List[float]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for documents in the vector store.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            embedding: Optional pre-computed embedding for the query
            
        Returns:
            List of search results with document content and metadata
        """
        try:
            # Perform the search
            if embedding:
                results = self.collection.query(
                    query_embeddings=[embedding],
                    n_results=limit,
                    where=filters
                )
            else:
                results = self.collection.query(
                    query_texts=[query],
                    n_results=limit,
                    where=filters
                )
            
            # Format results
            formatted_results = []
            if results["ids"] and results["ids"][0]:
                for i, doc_id in enumerate(results["ids"][0]):
                    formatted_results.append({
                        "id": doc_id,
                        "content": results["documents"][0][i],
                        "metadata": results["metadatas"][0][i],
                        "distance": results["distances"][0][i] if "distances" in results else None
                    })
            
            logger.info(f"Search for '{query}' returned {len(formatted_results)} results")
            return formatted_results
            
        except Exception as e:
            error_msg = f"Search failed: {str(e)}"
            logger.error(error_msg)
            raise SearchError(error_msg)
    
    def get_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a document by its ID.
        
        Args:
            document_id: The ID of the document to retrieve
            
        Returns:
            The document content and metadata, or None if not found
        """
        try:
            results = self.collection.get(ids=[document_id])
            
            if results["ids"]:
                return {
                    "id": results["ids"][0],
                    "content": results["documents"][0],
                    "metadata": results["metadatas"][0]
                }
            return None
            
        except Exception as e:
            error_msg = f"Failed to retrieve document {document_id}: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)
    
    def delete_document(self, document_id: str) -> None:
        """
        Delete a document from the vector store.
        
        Args:
            document_id: The ID of the document to delete
        """
        try:
            self.collection.delete(ids=[document_id])
            logger.info(f"Deleted document with ID {document_id}")
        except Exception as e:
            error_msg = f"Failed to delete document {document_id}: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)
    
    def delete_collection(self) -> None:
        """Delete the entire collection of code documents."""
        try:
            self.client.delete_collection(self.collection_name)
            logger.info(f"Deleted collection '{self.collection_name}'")
        except Exception as e:
            error_msg = f"Failed to delete collection: {str(e)}"
            logger.error(error_msg)
            raise VectorStoreError(error_msg)