import base64
import os
import sys
from dotenv import load_dotenv
import filetype

load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from preproc.utils.model_loaders import get_embedding as embed


def get_data_url(b64_data):
    # If it already has a comma, remove any existing data URI header
    if "," in b64_data:
        b64_data = b64_data.split(",")[1]

    # Decode to bytes to detect type
    img_bytes = base64.b64decode(b64_data)

    # Detect MIME type
    kind = filetype.guess(img_bytes)
    mime = kind.mime if kind else "application/octet-stream"

    # Return full data URI
    return f"data:{mime};base64,{b64_data}"


# Load JSON data and prepare node dictionaries with file-level metadata
def add_metadata(data):
    chunks = []

    # Extract file-level metadata to add to all chunks
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

        chunks.append(node_dict)

    return chunks


# Add embeddings to chunks using batch processing for performance
def add_embeddings(chunks, dimensions=1536):
    def process_batch(batch_data, modality):
        if batch_data:
            _indices, content = zip(*batch_data)
            embeddings = embed(content, modality, dimensions)
            for _index, embedding in zip(_indices, embeddings):
                chunks[_index]["embedding"] = embedding

    text_batch = [
        (index, chunk["text"])
        for index, chunk in enumerate(chunks)
        if chunk["type"] == "text"
    ]
    image_batch = [
        (index, get_data_url(chunk["image"]))
        for index, chunk in enumerate(chunks)
        if chunk["type"] == "image"
    ]

    process_batch(text_batch, "text")
    process_batch(image_batch, "image")

    return chunks
