import weaviate.classes as wvc
from weaviate.classes.config import ReferenceProperty, Reconfigure
from error_handlers import handle_collection_errors, handle_reference_errors


# Create single collection based on schema with optional multi-tenancy and quantization
@handle_collection_errors
def create_collection(
    client, name, schema, existing_collections, multi_tenancy=True, quantization=None
):
    if name in existing_collections:
        return

    properties = []
    for prop_name, data_type in schema.items():
        # Only enable search indexing for text properties
        searchable = prop_name == "text"
        properties.append(
            wvc.config.Property(
                name=prop_name, data_type=data_type, index_searchable=searchable
            )
        )

    client.collections.create(
        name=name,
        vectorizer_config=wvc.config.Configure.Vectorizer.none(),
        vector_index_config=wvc.config.Configure.VectorIndex.dynamic(
            quantizer=quantization
        ),
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(
            enabled=multi_tenancy, auto_tenant_creation=multi_tenancy
        ),
        properties=properties,
    )


# Delete collection from Weaviate instance
@handle_collection_errors
def delete_collection(client, name):
    client.collections.delete(name)


# Retrieve collection object by name
@handle_collection_errors
def get_collection(client, name):
    return client.collections.get(name)


# Update collection configuration including BM25, multi-tenancy, and vector index settings
@handle_collection_errors
def update_collection(
    client,
    name,
    bm25_k1=1.2,
    bm25_b=0.75,
    auto_tenant_creation=True,
    auto_tenant_activation=True,
    threshold=10000,
    hnsw_quantizer=None,
    flat_quantizer=None,
):
    client.collections.get(name).config.update(
        inverted_index_config=Reconfigure.inverted_index(
            bm25_b=bm25_b,
            bm25_k1=bm25_k1,
        ),
        multi_tenancy_config=Reconfigure.multi_tenancy(
            auto_tenant_creation=auto_tenant_creation,
            auto_tenant_activation=auto_tenant_activation,
        ),
        vector_index_config=Reconfigure.VectorIndex.dynamic(
            threshold=threshold,
            hnsw=Reconfigure.VectorIndex.hnsw(quantizer=get_quantizer(hnsw_quantizer)),
            flat=Reconfigure.VectorIndex.flat(quantizer=get_quantizer(flat_quantizer)),
        ),
    )


# Add reference property linking two collections
@handle_reference_errors
def add_reference(client, from_collection, ref_name, target_collection):
    collection = client.collections.get(from_collection)
    collection.config.add_reference(
        ReferenceProperty(name=ref_name, target_collection=target_collection)
    )


# Convert quantizer string to appropriate Reconfigure object
def get_quantizer(quantizer):
    if quantizer is None:
        return None

    quantizer_map = {
        "pq": Reconfigure.VectorIndex.Quantizer.pq(),
        "sq": Reconfigure.VectorIndex.Quantizer.sq(),
        "bq": Reconfigure.VectorIndex.Quantizer.bq(),
    }

    if quantizer not in quantizer_map:
        raise ValueError(f"Invalid quantizer: {quantizer}. Must be one of: pq, sq, bq")

    return quantizer_map[quantizer]
