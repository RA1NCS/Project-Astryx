from client import get_client
import json
from query import image_search, keyword_search, hybrid_search

client = get_client()

results = hybrid_search(
    client,
    collection_name="TextChunk",
    tenant_name="dev",
    query_text="What is desribed here?",
)
print(json.dumps(results, indent=4))
client.close()
