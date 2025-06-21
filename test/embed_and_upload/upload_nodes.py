import os
import sys
import base64
import json
import uuid
import warnings

from dotenv import load_dotenv
import weaviate
import weaviate.classes as wvc
from weaviate.classes.tenants import Tenant
from weaviate.classes.config import ReferenceProperty
from weaviate.classes.data import DataObject

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from preproc.utils.blob_utils import BlobStorageManager

load_dotenv()

# Suppress Pydantic deprecation warnings from Weaviate client
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=".*PydanticDeprecatedSince20.*",
)

_blob_mgr = BlobStorageManager(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

# Schema definitions - single source of truth
COMMON_SCHEMA = {
    "file_name": wvc.config.DataType.TEXT,
    "page": wvc.config.DataType.INT,
    "file_sha256": wvc.config.DataType.TEXT,
    "user": wvc.config.DataType.TEXT,
    "source_mime": wvc.config.DataType.TEXT,
    "total_nodes": wvc.config.DataType.INT,
    "chunk_id": wvc.config.DataType.TEXT,
}

TEXT_SCHEMA = {
    **COMMON_SCHEMA,
    "text": wvc.config.DataType.TEXT,
    "char_start": wvc.config.DataType.INT,
    "char_len": wvc.config.DataType.INT,
    "is_complex": wvc.config.DataType.BOOL,
    "has_tables": wvc.config.DataType.BOOL,
    "image_refs": wvc.config.DataType.TEXT_ARRAY,
    "ingestor": wvc.config.DataType.TEXT,
}

IMAGE_SCHEMA = {
    **COMMON_SCHEMA,
    "image_url": wvc.config.DataType.TEXT,
    "ingestor": wvc.config.DataType.TEXT,
}


# Create authenticated Weaviate cloud client connection
def get_client():
    return weaviate.connect_to_weaviate_cloud(
        cluster_url=os.getenv("WEAVIATE_URL"),
        auth_credentials=wvc.init.Auth.api_key(os.getenv("WEAVIATE_API_KEY")),
    )


# Initialize Weaviate collections with schemas and bidirectional references
def initialize_collections(client):
    existing_collections = set(client.collections.list_all().keys())

    # Create collection without references to avoid circular dependency
    def create_collection_without_refs(client, name, schema_dict):
        if name in existing_collections:
            return

        properties = []
        for prop_name, data_type in schema_dict.items():
            if prop_name == "text":
                properties.append(
                    wvc.config.Property(
                        name=prop_name, data_type=data_type, index_searchable=True
                    )
                )
            elif prop_name == "ingestor":
                properties.append(
                    wvc.config.Property(
                        name=prop_name, data_type=data_type, index_searchable=False
                    )
                )
            else:
                properties.append(
                    wvc.config.Property(name=prop_name, data_type=data_type)
                )

        client.collections.create(
            name=name,
            vectorizer_config=wvc.config.Configure.Vectorizer.none(),
            vector_index_config=wvc.config.Configure.VectorIndex.dynamic(),
            multi_tenancy_config=wvc.config.Configure.multi_tenancy(
                enabled=True, auto_tenant_creation=True
            ),
            properties=properties,
        )

    # Create base collections first
    if "TextChunk" not in existing_collections:
        create_collection_without_refs(client, "TextChunk", TEXT_SCHEMA)

    if "ImageChunk" not in existing_collections:
        create_collection_without_refs(client, "ImageChunk", IMAGE_SCHEMA)

    # Add bidirectional references after both collections exist
    if (
        "TextChunk" not in existing_collections
        or "ImageChunk" not in existing_collections
    ):
        try:
            text_collection = client.collections.get("TextChunk")
            text_collection.config.add_reference(
                ReferenceProperty(name="hasImages", target_collection="ImageChunk")
            )
        except Exception:
            pass

        try:
            image_collection = client.collections.get("ImageChunk")
            image_collection.config.add_reference(
                ReferenceProperty(name="belongsToText", target_collection="TextChunk")
            )
        except Exception:
            pass


# Map node types to Weaviate collection names
def get_collection_name(node_type):
    if node_type == "text":
        return "TextChunk"
    elif node_type == "image":
        return "ImageChunk"
    else:
        raise ValueError(f"Unknown node_type: {node_type}")


# Upload base64 image to Azure blob storage and return public URL
def upload_image(node):
    raw = node.get("image")

    if not raw:
        raise Exception("No image found to upload")

    try:
        data = base64.b64decode(raw)
        username = node["metadata"]["user"]
        blob_name = f"{username}/images/{node['id_']}.png"
        return _blob_mgr.upload_processed_file(
            blob_name, data, tags={"status": "indexed"}, content_type="image/png"
        )

    except Exception as e:
        raise Exception(f"Failed to upload image: {e}")


# Build Weaviate properties dict from node data using direct extraction
def build_properties(node):
    # Get base properties from metadata and node
    properties = {
        "chunk_id": node["id_"],
        "file_name": node["metadata"].get("file_name"),
        "page": node["metadata"].get("page"),
        "file_sha256": node["metadata"].get("file_sha256"),
        "user": node["metadata"].get("user"),
        "source_mime": node["metadata"].get("source_mime"),
        "total_nodes": node["metadata"].get("total_nodes"),
        "ingestor": node["metadata"].get("ingestor"),
    }

    # Add type-specific properties
    if node["type"] == "image":
        properties["image_url"] = upload_image(node)
    else:  # text
        properties.update(
            {
                "text": node.get("text"),
                "char_start": node["metadata"].get("char_start"),
                "char_len": node["metadata"].get("char_len"),
                "is_complex": node["metadata"].get("is_complex"),
                "has_tables": node["metadata"].get("has_tables"),
                "image_refs": node["metadata"].get("image_refs", []),
            }
        )

    return properties


# Convert human-readable ID to deterministic UUID5 hash for Weaviate
def ensure_uuid(raw_id):
    if len(raw_id) == 36 and raw_id.count("-") == 4:
        return raw_id
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


# Process embedded nodes into Weaviate DataObjects grouped by collection
def process_nodes(node_dicts):
    # Extract username from first node
    username = node_dicts[0]["metadata"]["user"]

    collections = {
        "TextChunk": {"objs": [], "tenant": username},
        "ImageChunk": {"objs": [], "tenant": username},
    }

    for n in node_dicts:
        class_name = get_collection_name(n["type"])

        props = build_properties(n)

        node_uuid = ensure_uuid(n["id_"])
        vec = n.get("embedding")

        data_obj = DataObject(
            uuid=node_uuid,
            properties=props,
            vector=vec,
        )

        collections[class_name]["objs"].append(data_obj)

    return collections


# Create multi-tenant collection tenant if it doesn't exist
def ensure_tenant(col, tenant):
    try:
        names = {t.name for t in col.tenants.get()}
        if tenant not in names:
            col.tenants.create([Tenant(name=tenant)])
    except Exception:
        pass


# Batch upload objects to Weaviate collections without cross-references
def upload_objects_without_references(client, collections):
    for class_name, data in collections.items():
        col = client.collections.get(class_name).with_tenant(tenant=data["tenant"])

        # Create objects without references
        objects_without_refs = []
        for obj in data["objs"]:
            obj_copy = DataObject(
                uuid=obj.uuid,
                properties=obj.properties,
                vector=obj.vector,
            )
            objects_without_refs.append(obj_copy)

        # Upload in batch
        result = col.data.insert_many(objects_without_refs)

        if hasattr(result, "errors") and result.errors:
            print(f"Upload errors for {class_name}: {result.errors}")

        print(f"Uploaded {len(objects_without_refs)} {class_name} objects")


# Convert relationship node IDs to UUIDs for cross-references
def convert_relationships_to_uuid(relationships):
    converted = {}
    for rel_key, rel_data in relationships.items():
        if isinstance(rel_data, dict) and "node_id" in rel_data:
            converted[rel_key] = {
                **rel_data,
                "node_id": ensure_uuid(rel_data["node_id"]),
            }
    return converted


# Build bidirectional cross-references between text and image nodes
def build_references(node):
    if node["type"] == "image":
        # Build references from Image to Text using original relationships
        relationships = node.get("relationships", {})
        text_refs = []

        for rel_data in relationships.values():
            if isinstance(rel_data, dict) and "node_id" in rel_data:
                text_refs.append(ensure_uuid(rel_data["node_id"]))

        return {"belongsToText": text_refs} if text_refs else {}

    else:  # text
        # Build references from Text to Image using image_refs
        image_refs = node["metadata"].get("image_refs", [])
        return (
            {"hasImages": [ensure_uuid(img_ref) for img_ref in image_refs]}
            if image_refs
            else {}
        )


# Add bidirectional cross-references between uploaded objects
def add_references(client, node_dicts):
    # Extract username from first node
    username = node_dicts[0]["metadata"]["user"]

    for n in node_dicts:
        node_uuid = ensure_uuid(n["id_"])
        class_name = get_collection_name(n["type"])

        refs = build_references(n)
        if refs:
            col = client.collections.get(class_name).with_tenant(tenant=username)

            for ref_name, ref_uuids in refs.items():
                for target_uuid in ref_uuids:
                    try:
                        col.data.reference_add(
                            from_uuid=node_uuid, from_property=ref_name, to=target_uuid
                        )
                    except Exception as e:
                        print(
                            f"Error adding reference to {class_name} {node_uuid}: {e}"
                        )


# Upload embedded nodes to Weaviate with bidirectional cross-references
def ingest(node_dicts):
    if not node_dicts:
        raise ValueError("No nodes provided for ingestion")

    # Extract username from node metadata
    username = node_dicts[0]["metadata"].get("user")
    if not username:
        raise ValueError("No username found in node metadata")

    client =  ()

    initialize_collections(client)

    collections = process_nodes(node_dicts)

    # First pass: Upload all objects without references
    upload_objects_without_references(client, collections)

    # Second pass: Add references
    add_references(client, node_dicts)

    client.close()
