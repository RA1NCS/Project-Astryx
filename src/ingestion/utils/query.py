import sys, os, base64, requests
from weaviate.classes.query import MetadataQuery
from weaviate.classes.query import QueryReference
from weaviate.classes.query import Filter

try:
    # Try relative imports (when run from outside utils)
    from .error_handlers import handle_query_errors
except ImportError:
    # Fall back to absolute imports (when run from inside utils)
    from error_handlers import handle_query_errors

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))
from preproc.utils.model_loaders import get_embedding


# Get tenant-specific collection instance for query operations
@handle_query_errors
def get_tenant_collection(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    return collection.with_tenant(tenant_name)


# Convert Weaviate search results to serializable dictionary format
def convert_to_dict(result):
    converted_result = []
    for item in result:
        item.metadata.creation_time = str(item.metadata.creation_time)
        item.metadata.last_update_time = str(item.metadata.last_update_time)
        item.references = {
            reference_name: [
                {
                    str(referenced_object.uuid): referenced_object.properties[
                        "chunk_id"
                    ],
                }
                for referenced_object in reference_objects.objects
            ]
            for reference_name, reference_objects in item.references.items()
        }

        converted_result.append(
            {
                "uuid": str(item.uuid),
                "metadata": item.metadata.__dict__,
                "properties": item.properties,
                "references": item.references,
                "vector": item.vector,
                "collection": item.collection,
            }
        )
    return converted_result


# Convert simple filter dictionary to Weaviate Filter object
def create_simple_filter(filters):
    if not filters:
        return None

    filter_conditions = []
    for property_name, value in filters.items():
        filter_conditions.append(Filter.by_property(property_name).equal(value))

    if len(filter_conditions) == 1:
        return filter_conditions[0]
    elif len(filter_conditions) > 1:
        # Combine multiple filters with AND
        result = filter_conditions[0]
        for condition in filter_conditions[1:]:
            result = result & condition
        return result

    return None


# Perform BM25 keyword search on tenant-specific collection
@handle_query_errors
def keyword_search(
    client,
    collection_name,
    tenant_name,
    query_text,
    limit=5,
    get_vector=False,
    filters=None,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    user_filters = create_simple_filter(filters)

    return convert_to_dict(
        tenant_collection.query.bm25(
            query=query_text,
            limit=limit,
            return_metadata=MetadataQuery.full(),
            return_references=QueryReference(link_on="hasImages"),
            include_vector=get_vector,
            filters=user_filters,
        ).objects
    )


# Perform semantic vector search on tenant-specific collection
@handle_query_errors
def vector_search(
    client,
    collection_name,
    tenant_name,
    query_text,
    limit=5,
    get_vector=False,
    filters=None,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    user_filters = create_simple_filter(filters)

    return convert_to_dict(
        tenant_collection.query.near_vector(
            near_vector=get_embedding(query_text, "text", 1536),
            limit=limit,
            return_metadata=MetadataQuery.full(),
            return_references=QueryReference(link_on="hasImages"),
            include_vector=get_vector,
            filters=user_filters,
        ).objects
    )


# Perform hybrid search combining keyword and vector search
@handle_query_errors
def hybrid_search(
    client,
    collection_name,
    tenant_name,
    query_text,
    alpha=0.75,
    limit=5,
    get_vector=False,
    filters=None,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    user_filters = create_simple_filter(filters)

    return convert_to_dict(
        tenant_collection.query.hybrid(
            query=query_text,
            vector=get_embedding(query_text, "text", 1536),
            alpha=alpha,
            limit=limit,
            return_metadata=MetadataQuery.full(),
            return_references=QueryReference(
                link_on="hasImages", return_properties=["chunk_id"]
            ),
            include_vector=get_vector,
            filters=user_filters,
        ).objects
    )


# Find similar objects based on an existing object's vector
@handle_query_errors
def near_object_search(
    client,
    collection_name,
    tenant_name,
    object_uuid,
    limit=5,
    get_vector=False,
    filters=None,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    user_filters = create_simple_filter(filters)

    return convert_to_dict(
        tenant_collection.query.near_object(
            near_object=object_uuid,
            limit=limit,
            return_metadata=MetadataQuery.full(),
            return_references=QueryReference(
                link_on="hasImages", return_properties=["chunk_id"]
            ),
            include_vector=get_vector,
            filters=user_filters,
        ).objects
    )
