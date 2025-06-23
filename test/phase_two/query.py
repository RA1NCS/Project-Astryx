from embed import Embedder
from weaviate.classes.query import MetadataQuery


def get_tenant_collection(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    return collection.with_tenant(tenant_name)


def convert_to_dict(result):
    converted_result = []
    for item in result:
        item.metadata.creation_time = str(item.metadata.creation_time)
        item.metadata.last_update_time = str(item.metadata.last_update_time)

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


def keyword_search(client, collection_name, tenant_name, query_text, limit=5):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.bm25(
            query=query_text,
            limit=limit,
            return_metadata=MetadataQuery.full(),
        ).objects
    )


def vector_search(client, collection_name, tenant_name, query_text, limit=5):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.near_vector(
            near_vector=Embedder().get_embedding(query_text),
            limit=limit,
            return_metadata=MetadataQuery.full(),
        ).objects
    )


def hybrid_search(
    client,
    collection_name,
    tenant_name,
    query_text,
    alpha=0.75,
    limit=5,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.hybrid(
            query=query_text,
            vector=Embedder().get_embedding(query_text),
            alpha=alpha,
            limit=limit,
            return_metadata=MetadataQuery.full(),
        ).objects
    )
