import json
from client import get_client
from schema import TEXT_SCHEMA, IMAGE_SCHEMA
import collection
import tenant


# Initialize Image & Text collections with references
def initialize_collections(client):
    existing_collections = set(client.collections.list_all().keys())

    # Create Collections
    collection.create_collection(
        client,
        "TextChunk",
        TEXT_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )
    collection.create_collection(
        client,
        "ImageChunk",
        IMAGE_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )

    # Add references
    if (
        "TextChunk" not in existing_collections
        or "ImageChunk" not in existing_collections
    ):
        collection.add_reference(client, "TextChunk", "hasImages", "ImageChunk")
        collection.add_reference(client, "ImageChunk", "belongsToText", "TextChunk")


# Upload embedded nodes to Weaviate with bidirectional cross-references
def ingest(chunks):
    if not chunks:
        raise ValueError("No nodes provided for ingestion")

    # Extract username from node metadata
    username = chunks[0]["metadata"].get("user")
    if not username:
        raise ValueError("No username found in node metadata")

    client = get_client()

    initialize_collections(client)

    collections = process_nodes(chunks)

    # First pass: Upload all objects without references
    upload_objects_without_references(client, collections)

    # Second pass: Add references
    add_references(client, chunks)

    client.close()