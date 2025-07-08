import uuid
from weaviate.classes.query import QueryReference

try:
    # Try relative imports (when run from outside utils)
    from .collection import get_collection_with_tenant, get_collection
    from .error_handlers import handle_object_errors
    from .query import convert_to_dict
except ImportError:
    # Fall back to absolute imports (when run from inside utils)
    from collection import get_collection_with_tenant, get_collection
    from error_handlers import handle_object_errors
    from query import convert_to_dict


# Get reference property names from collection schema dynamically
def get_reference_properties(collection):
    try:
        config = collection.config.get()
        reference_props = []

        # Reference properties are stored in the references attribute
        if hasattr(config, "references") and config.references:
            for ref in config.references:
                reference_props.append(ref.name)

        return reference_props
    except Exception:
        return []


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
    get_references=False,
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

        # Get references if requested
        if get_references:
            try:
                # Get reference property names dynamically from collection schema
                ref_properties = get_reference_properties(tenant_collection)

                if ref_properties:
                    # Fetch the object with references for each reference property
                    all_references = {}
                    for ref_prop in ref_properties:
                        try:
                            obj_with_refs = tenant_collection.query.fetch_object_by_id(
                                chunks.uuid,
                                return_references=QueryReference(link_on=ref_prop),
                            )

                            if (
                                hasattr(obj_with_refs, "references")
                                and obj_with_refs.references
                            ):
                                # Merge references from this property
                                all_references.update(obj_with_refs.references)
                        except Exception:
                            continue

                    if all_references:
                        # Convert _CrossReference objects to JSON-serializable format
                        serializable_refs = {}
                        for ref_prop, ref_obj in all_references.items():
                            if hasattr(ref_obj, "objects"):
                                # Extract UUIDs from reference objects
                                ref_uuids = [str(obj.uuid) for obj in ref_obj.objects]
                                serializable_refs[ref_prop] = ref_uuids
                            else:
                                # Fallback: try to convert to string
                                serializable_refs[ref_prop] = str(ref_obj)

                        chunk["references"] = serializable_refs
            except Exception:
                # If reference retrieval fails, continue without references
                pass

        objects.append(chunk)

    return objects


@handle_object_errors
def get_object(
    client,
    collection_name,
    tenant_name,
    object_uuid,
    get_vectors=False,
    return_json=False,
):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)

    # Get reference property names dynamically from collection schema
    ref_properties = get_reference_properties(tenant_collection)

    # Fetch object with all available references
    if ref_properties:
        # Use the first reference property or fetch separately if multiple
        fetched_object = tenant_collection.query.fetch_object_by_id(
            object_uuid,
            include_vector=get_vectors,
            return_references=QueryReference(link_on=ref_properties[0]),
        )
    else:
        fetched_object = tenant_collection.query.fetch_object_by_id(
            object_uuid,
            include_vector=get_vectors,
        )

    return convert_to_dict([fetched_object]) if return_json else fetched_object


def get_object_metadata(client, collection_name, tenant_name, object_uuid):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)

    # Get reference property names dynamically from collection schema
    ref_properties = get_reference_properties(tenant_collection)

    # Fetch object metadata with references if available
    if ref_properties:
        obj = tenant_collection.query.fetch_object_by_id(
            object_uuid,
            return_references=QueryReference(link_on=ref_properties[0]),
        )
    else:
        obj = tenant_collection.query.fetch_object_by_id(object_uuid)

    return obj.properties


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


# Get references for a specific object
@handle_object_errors
def get_object_references(client, collection_name, tenant_name, object_uuid):
    if tenant_name:
        tenant_collection = get_collection_with_tenant(
            client, collection_name, tenant_name
        )
    else:
        tenant_collection = get_collection(client, collection_name)

    try:
        # Get reference property names dynamically from collection schema
        ref_properties = get_reference_properties(tenant_collection)

        if not ref_properties:
            return {}

        # Fetch references for each reference property
        all_references = {}
        for ref_prop in ref_properties:
            try:
                obj = tenant_collection.query.fetch_object_by_id(
                    object_uuid, return_references=QueryReference(link_on=ref_prop)
                )
                if hasattr(obj, "references") and obj.references:
                    all_references.update(obj.references)
            except Exception:
                continue

        return all_references
    except Exception:
        return {}
