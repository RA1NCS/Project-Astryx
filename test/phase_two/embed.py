import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from preproc.utils.model_loaders import create_embedding_model


# Handles embedding generation and metadata processing for document nodes
class Embedder:
    def __init__(self, embedding_dim=1536):
        self.embedding_dim = embedding_dim
        self.embedding_model = create_embedding_model(embedding_dim)

    # Load JSON data and prepare node dictionaries with file-level metadata
    def add_metadata(self, data):
        nodes = []

        # Extract file-level metadata to add to all nodes
        file_metadata = {
            "file_sha256": data.get("file_sha256"),
            "user": data.get("user"),
            "source_mime": data.get("source_mime"),
            "total_nodes": data.get("total_nodes"),
        }

        for chunk in data["nodes"]:
            # Add file-level metadata to node metadata
            chunk["metadata"].update(file_metadata)

            # Create node dict keeping original type names
            if chunk.get("type") == "image":
                node_dict = {
                    "id_": chunk["id_"],
                    "type": "image",
                    "metadata": chunk["metadata"],
                    "image": chunk.get("image", ""),
                    "relationships": chunk.get("relationships", {}),
                    "embedding": None,
                }
            elif chunk.get("type") == "text":
                node_dict = {
                    "id_": chunk["id_"],
                    "type": "text",
                    "text": chunk.get("text", ""),
                    "metadata": chunk["metadata"],
                    "relationships": chunk.get("relationships", {}),
                    "embedding": None,
                }
            else:
                raise ValueError(f"Unknown chunk type: {chunk.get('type')}")

            nodes.append(node_dict)

        return nodes

    # Add embeddings to node dictionaries using unified embedding model
    def add_embeddings(self, data):
        for chunk in data:
            chunk_type = chunk["type"]
            assert chunk_type in [
                "image",
                "text",
            ], f"Unknown chunk type: {chunk.get('type')}"

            chunk["embedding"] = self.embedding_model.get_text_embedding(
                chunk[chunk_type]
            )

        return data
