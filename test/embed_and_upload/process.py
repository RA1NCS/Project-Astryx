import sys, os
from weaviate.classes.data import DataObject

from utils.objects import generate_uuid

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from preproc.utils.blob_utils import BlobStorageManager


# Convert chunk data into Weaviate properties with type-specific handling
def build_properties(chunk, image_map=None):
    # Get base properties from metadata and chunk
    properties = {
        "chunk_id": chunk["id_"],
        "file_name": chunk["metadata"].get("file_name"),
        "page": chunk["metadata"].get("page"),
        "file_sha256": chunk["metadata"].get("file_sha256"),
        "user": chunk["metadata"].get("user"),
        "source_mime": chunk["metadata"].get("source_mime"),
        "total_chunks": chunk["metadata"].get("total_chunks"),
    }

    # Add type-specific properties
    if chunk["type"] == "image":
        properties["image_url"] = image_map.get(chunk["id_"]) if image_map else None
    else:  # text
        properties.update(
            {
                "text": chunk.get("text"),
                "char_start": chunk["metadata"].get("char_start"),
                "char_len": chunk["metadata"].get("char_len"),
                "is_complex": chunk["metadata"].get("is_complex"),
                "has_tables": chunk["metadata"].get("has_tables"),
                "image_refs": chunk["metadata"].get("image_refs", []),
            }
        )

    return properties


# Batch upload image chunks to Azure blob storage
def upload_images_batch(chunks):
    blob_storage = BlobStorageManager(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

    image_chunks = [chunk for chunk in chunks if chunk["type"] == "image"]
    if not image_chunks:
        return {}

    upload_images = [
        {
            "tenant": chunk["metadata"]["user"],
            "image_base64": chunk["image"],
            "image_id": chunk["id_"],
        }
        for chunk in image_chunks
    ]

    image_urls = blob_storage.upload_images_batch(upload_images)

    return {chunk["id_"]: url for chunk, url in zip(image_chunks, image_urls)}


# Transform chunks into DataObjects grouped by collection type
def process_chunks(chunks):
    # Extract username from first chunk
    username = chunks[0]["metadata"]["user"]

    # Batch upload all images first
    image_map = upload_images_batch(chunks)

    collections = {
        "TextChunk": {"objs": [], "tenant": username},
        "ImageChunk": {"objs": [], "tenant": username},
    }

    for chunk in chunks:
        class_name = (
            "ImageChunk"
            if chunk["type"] == "image"
            else "TextChunk" if chunk["type"] == "text" else None
        )

        props = build_properties(chunk, image_map)

        chunk_uuid = generate_uuid(chunk["id_"])
        vec = chunk.get("embedding")

        data_obj = DataObject(
            uuid=chunk_uuid,
            properties=props,
            vector=vec,
        )

        collections[class_name]["objs"].append(data_obj)

    return collections
