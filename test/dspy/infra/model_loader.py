from os import getenv
from dotenv import load_dotenv

load_dotenv()

from dspy import LM
from dspy.retrieve.weaviate_rm import WeaviateRM
from weaviate import connect_to_weaviate_cloud
import weaviate.classes as wvc
from weaviate import WeaviateClient
from replicate import Client as ReplicateClient
from litellm import embedding


# Weaviate
def get_weaviate_client(cluster_url: str = None, api_key: str = None) -> WeaviateClient:
    return connect_to_weaviate_cloud(
        cluster_url=cluster_url or getenv("WEAVIATE_URL"),
        auth_credentials=wvc.init.Auth.api_key(api_key or getenv("WEAVIATE_API_KEY")),
    )


def get_weaviate_rm(
    collection_name: str,
    tenant_id: str,
    key: str,
    cluster_url: str = None,
    api_key: str = None,
) -> WeaviateRM:
    return WeaviateRM(
        weaviate_client=get_weaviate_client(cluster_url, api_key),
        weaviate_collection_name=collection_name,
        tenant_id=tenant_id,
        weaviate_collection_text_key=key,
    )


# LLM
def get_llm(
    model_name: str, api_key: str = None, api_base: str = None, api_version: str = None
) -> LM:
    return LM(
        model_name,
        api_key=api_key or getenv("OPENAI_API_KEY"),
        api_base=api_base or getenv("OPENAI_API_BASE"),
        api_version=api_version or getenv("OPENAI_API_VERSION"),
)


# Embedding
def get_text_embedding(
    input,
    dimensions=1536,
    model_name: str = None,
    api_key: str = None,
    api_base: str = None,
    api_version: str = None,
) -> list[list[float]]:
    response = embedding(
        model=model_name or "azure_ai/embed-v-4-0",
        input=input,
        dimensions=dimensions,
        api_key=api_key or getenv("AZURE_EMBEDDING_KEY"),
        api_base=api_base or getenv("AZURE_EMBEDDING_ENDPOINT"),
        api_version=api_version or getenv("AZURE_EMBEDDING_API_VERSION"),
    )
    return [item["embedding"] for item in response["data"]]


# Replicate
def run_replicate(input: dict, model_name: str, api_key: str = None) -> dict:
    client = ReplicateClient(api_key=api_key or getenv("REPLICATE_API_TOKEN"))
    return client.run(
        model_name,
        input=input,
        api_key=api_key or getenv("REPLICATE_API_TOKEN"),
    )
