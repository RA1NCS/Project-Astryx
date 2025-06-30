from os import getenv
from weaviate import connect_to_weaviate_cloud
import weaviate.classes as wvc
from dotenv import load_dotenv

load_dotenv()


# Create authenticated Weaviate cloud client connection
def get_client():
    return connect_to_weaviate_cloud(
        cluster_url=getenv("WEAVIATE_URL"),
        auth_credentials=wvc.init.Auth.api_key(getenv("WEAVIATE_API_KEY")),
    )
