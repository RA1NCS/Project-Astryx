from llama_index.embeddings.litellm import LiteLLMEmbedding
import os


def create_embedding_model(dimensions):
    return LiteLLMEmbedding(
        model_name="azure_ai/embed-v-4-0",
        api_key=os.getenv("AZURE_EMBEDDING_KEY"),
        api_base=os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        api_version="2024-10-21",
        dimensions=dimensions,
    )
