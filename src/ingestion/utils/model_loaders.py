import os, base64
from litellm import embedding
from dotenv import load_dotenv
from azure.ai.inference import ImageEmbeddingsClient, models
from azure.core.credentials import AzureKeyCredential


load_dotenv()


# Unified embedding function supporting both single and batch inputs
def get_embedding(input, modality, dimensions):
    if modality == "text":
        if isinstance(input, str):
            input = [input]
            return get_text_embedding(input, dimensions)[0]
        return get_text_embedding(input, dimensions)
    elif modality == "image":
        return get_image_embedding(input, dimensions)
    else:
        raise ValueError(f"Invalid input: {modality}")


# Batch text embeddings function
def get_text_embedding(input, dimensions):
    response = embedding(
        model="azure_ai/embed-v-4-0",
        input=input,
        dimensions=dimensions,
        api_key=os.getenv("AZURE_EMBEDDING_KEY"),
        api_base=os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        api_version="2024-10-21",
    )
    return [item["embedding"] for item in response["data"]]


# Batch image embeddings using Azure AI true batch API
def get_image_embedding(input, dimensions):
    client = ImageEmbeddingsClient(
        endpoint=os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_EMBEDDING_KEY")),
        model="embed-v-4-0",
        dimensions=dimensions,
    )

    images = [models.ImageEmbeddingInput(image=image) for image in input]
    response = client.embed(input=images)

    return [item.embedding for item in response.data]
