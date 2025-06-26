import json
from process import process_chunks

from utils.collection import (
    create_collection,
    add_reference,
    get_collection_with_tenant,
    get_collection_name,
)
from utils.client import get_client
from utils.schema import TEXT_SCHEMA, IMAGE_SCHEMA
from utils.objects import batch_upload_objects, generate_uuid
from embed import add_metadata, add_embeddings


# Create TextChunk and ImageChunk collections with bidirectional references
def initialize_collections(client):
    existing_collections = set(client.collections.list_all().keys())

    # Create Collections
    create_collection(
        client,
        "TextChunk",
        TEXT_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )
    create_collection(
        client,
        "ImageChunk",
        IMAGE_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )

    # Add references only for newly created collections
    if (
        "TextChunk" not in existing_collections
        or "ImageChunk" not in existing_collections
    ):
        add_reference(client, "TextChunk", "hasImages", "ImageChunk")
        add_reference(client, "ImageChunk", "belongsToText", "TextChunk")


# Batch upload processed chunks to their respective collections
def batch_upload_chunks(client, chunks):
    for collection_name, data in chunks.items():
        # Convert DataObjects to format expected by batch upload utility
        objects_for_batch = []
        for obj in data["objs"]:
            objects_for_batch.append(
                {
                    "uuid": obj.uuid,
                    "properties": obj.properties,
                    "vector": obj.vector,
                }
            )

        # Delegate to batch upload function
        batch_upload_objects(
            client=client,
            collection_name=collection_name,
            objects=objects_for_batch,
            tenant_name=data["tenant"],
        )


# Extract reference UUIDs from chunk relationships or metadata
def get_reference_uuids(chunk):
    if chunk["type"] == "image":
        return [
            generate_uuid(rel["chunk_id"])
            for rel in chunk.get("relationships", {}).values()
            if isinstance(rel, dict) and "chunk_id" in rel
        ]
    return [generate_uuid(ref) for ref in chunk["metadata"].get("image_refs", [])]


# Establish cross-references between text and image chunks
def add_references(client, chunks):
    username = chunks[0]["metadata"]["user"]

    for chunk in chunks:
        ref_uuids = get_reference_uuids(chunk)
        if not ref_uuids:
            continue

        collection = get_collection_with_tenant(
            client, get_collection_name(chunk["type"]), username
        )
        ref_name = "belongsToText" if chunk["type"] == "image" else "hasImages"

        for target_uuid in ref_uuids:
            try:
                collection.data.reference_add(
                    from_uuid=generate_uuid(chunk["id_"]),
                    from_property=ref_name,
                    to=target_uuid,
                )
            except Exception:
                pass


# Process and upload chunks to Weaviate with complete cross-reference support
def ingest(chunks):
    if not chunks:
        raise ValueError("No chunks provided for ingestion")

    # Extract username from chunk metadata
    username = chunks[0]["metadata"].get("user")

    if not username:
        raise ValueError("No username found in chunk metadata")

    client = get_client()
    initialize_collections(client)
    processed_chunks = process_chunks(chunks)
    batch_upload_chunks(client, processed_chunks)
    add_references(client, chunks)

    client.close()


# Embed then Upload to Weaviate
def main():
    with open("output/input.json", "r", encoding="utf-8") as fp:
        data = json.load(fp)
    nodes = add_metadata(data)
    nodes = add_embeddings(nodes)
    ingest(nodes)

    # Save embedded nodes to JSON
    # with open("output/output.json", "w") as f:
    #     json.dump(nodes, f, indent=4)


if __name__ == "__main__":
    main()
