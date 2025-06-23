import json
from embed import Embedder
from weaviate.classes.query import MetadataQuery


def get_tenant_collection(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    return collection.with_tenant(tenant_name)


# Convert Weaviate result objects to JSON-serializable dictionaries
def convert_to_dict(result):
    converted_result = []
    for item in result:
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


def keyword_search(
    client, collection_name, tenant_name, query_text, limit=5, give_scores=True
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.bm25(
            query=query_text,
            limit=limit,
            return_metadata=MetadataQuery(score=give_scores, explain_score=give_scores),
        ).objects
    )


def vector_search(
    client, collection_name, tenant_name, query_text, limit=5, give_scores=True
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.near_vector(
            near_vector=Embedder().get_embedding(query_text),
            limit=limit,
            return_metadata=MetadataQuery(distance=give_scores, certainty=give_scores),
        ).objects
    )


def hybrid_search(
    client,
    collection_name,
    tenant_name,
    query_text,
    alpha=0.75,
    limit=5,
    give_scores=True,
):
    tenant_collection = get_tenant_collection(client, collection_name, tenant_name)

    return convert_to_dict(
        tenant_collection.query.hybrid(
            query=query_text,
            vector=Embedder().get_embedding(query_text),
            alpha=alpha,
            limit=limit,
            return_metadata=MetadataQuery(
                score=give_scores,
                explain_score=give_scores,
                distance=give_scores,
                certainty=give_scores,
            ),
        ).objects
    )
