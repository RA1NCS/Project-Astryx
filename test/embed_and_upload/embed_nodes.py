import os
from dotenv import load_dotenv
from llama_index.embeddings.litellm import LiteLLMEmbedding

load_dotenv()

EMBEDDING_DIM = 1536


# Create embedding model with unified 1536 dimensions for both text and images
def create_embedding_model():
    return LiteLLMEmbedding(
        model_name="azure_ai/embed-v-4-0",
        api_key=os.getenv("AZURE_EMBEDDING_KEY"),
        api_base=os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        api_version="2024-10-21",
        dimensions=EMBEDDING_DIM,
    )


# Load JSON data and prepare node dictionaries with file-level metadata
def add_metadata(data):
    nodes = []

    # Extract file-level metadata to add to all nodes
    file_metadata = {
        "file_sha256": data.get("file_sha256"),
        "user": data.get("user"),
        "source_mime": data.get("source_mime"),
        "total_nodes": data.get("total_nodes"),
        "ingestor": "llamaindex",
    }

    for nd in data["nodes"]:
        # Add file-level metadata to node metadata
        nd["metadata"].update(file_metadata)

        # Create node dict keeping original type names
        if nd.get("type") == "image":
            node_dict = {
                "id_": nd["id_"],
                "type": "image",
                "metadata": nd["metadata"],
                "image": nd.get("image", ""),
                "relationships": nd.get("relationships", {}),
                "embedding": None,
            }
        else:
            node_dict = {
                "id_": nd["id_"],
                "type": "text",
                "text": nd.get("text", ""),
                "metadata": nd["metadata"],
                "relationships": nd.get("relationships", {}),
                "embedding": None,
            }

        nodes.append(node_dict)

    return nodes


# Add embeddings to node dictionaries using unified embedding model
def add_embeddings(nodes):
    embedding_model = create_embedding_model()

    for node in nodes:
        if node["type"] == "image":
            node["embedding"] = embedding_model.get_text_embedding(node["image"])
        elif node["type"] == "text" and node["text"].strip():
            node["embedding"] = embedding_model.get_text_embedding(node["text"])

    return nodes
