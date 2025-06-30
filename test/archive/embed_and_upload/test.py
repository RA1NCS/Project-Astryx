from utils import collection
from utils.client import get_client
import utils.objects as objects
import utils.query as query
import json

client = get_client()
print(
    json.dumps(
        query.hybrid_search(
            client,
            "TextChunk",
            "dev",
            "What are the graphs for each of the images in the Physics Lab 4? How many images are there? What are the slopes of each of them?",
            limit=5,
        )
    )
)

client.close()
