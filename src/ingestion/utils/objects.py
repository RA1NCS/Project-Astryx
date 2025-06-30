import uuid
from weaviate.classes.query import QueryReference

try:
    # Try relative imports (when run from outside utils)
    from .collection import get_collection_with_tenant, get_collection
    from .error_handlers import handle_object_errors
except ImportError:
    # Fall back to absolute imports (when run from inside utils)
    from collection import get_collection_with_tenant, get_collection
    from error_handlers import handle_object_errors


# Generate deterministic UUID from string input for consistent object identification
def generate_uuid(raw_id):
    if len(raw_id) == 36 and raw_id.count("-") == 4:
        return raw_id
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


# Retrieve all objects from a tenant-specific collection with optional metadata
@handle_object_errors
def get_objects(
    client,
    collection_name,
    tenant_name,
    get_properties=False,
    get_vectors=False,
):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        # For single-tenant collections, get collection directly
        tenant_collection = get_collection(client, collection_name)

    objects = []
    for chunks in tenant_collection.iterator(include_vector=get_vectors):
        chunk = {"uuid": str(chunks.uuid)}
        if get_properties:
            chunk["properties"] = chunks.properties
        if get_vectors:
            chunk["vector"] = chunks.vector
        objects.append(chunk)

    return objects


@handle_object_errors
def get_object(
    client,
    collection_name,
    tenant_name,
    object_uuid,
    get_vectors=False,
):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)
    return tenant_collection.query.fetch_object_by_id(
        object_uuid,
        include_vector=get_vectors,
        return_references=QueryReference(link_on="hasImages"),
    )


def get_object_metadata(client, collection_name, tenant_name, object_uuid):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)
    return tenant_collection.query.fetch_object_by_id(
        object_uuid,
        return_references=QueryReference(link_on="hasImages"),
    ).properties


# Update specific properties of an object in a tenant-specific collection
@handle_object_errors
def update_object(
    client,
    collection_name,
    tenant_name,
    object_uuid,
    properties,
    vector=None,
):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)
    return tenant_collection.data.update(
        uuid=object_uuid,
        properties=properties,
        vector=vector,
    )


# Batch upload objects to a tenant-specific collection using modern Weaviate API
@handle_object_errors
def batch_upload_objects(
    client,
    collection_name,
    objects,
    tenant_name,
    batch_size=100,
):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        # For single-tenant collections, get collection directly
        tenant_collection = get_collection(client, collection_name)

    uploaded_count = 0

    with tenant_collection.batch.fixed_size(batch_size=batch_size) as batch:
        for obj in objects:
            batch.add_object(
                properties=obj.get("properties"),
                uuid=obj.get("uuid"),
                vector=obj.get("vector"),
            )

            uploaded_count += 1

    # Report any failed objects after batch completion
    failed_objects = tenant_collection.batch.failed_objects
    if failed_objects:
        print(f"Failed to upload {len(failed_objects)} objects to {collection_name}")
        return uploaded_count, failed_objects

    return uploaded_count, []


# Delete an object from a tenant-specific collection
@handle_object_errors
def delete_object(client, collection_name, tenant_name, object_uuid):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)
    return tenant_collection.objects.delete_by_id(object_uuid)
