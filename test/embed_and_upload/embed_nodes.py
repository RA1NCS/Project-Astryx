import os
import json
from dotenv import load_dotenv
from llama_index.core.schema import TextNode, ImageNode
from llama_index.embeddings.litellm import LiteLLMEmbedding
from output_utils import output

load_dotenv()


def create_embedding_model(dimensions):
    return LiteLLMEmbedding(
        model_name="azure_ai/embed-v-4-0",
        api_key=os.getenv("AZURE_EMBEDDING_KEY"),
        api_base=os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        api_version="2024-10-21",
        dimensions=dimensions,
    )


def load_nodes_from_json(file_path):
    nodes = []
    data = json.loads(open(file_path).read())

    for nd in data["nodes"]:
        nd["metadata"]["ingestor"] = "llamaindex"

        if nd.get("type") == "image":
            node = ImageNode(
                id_=nd["id_"],
                metadata=nd["metadata"],
                image=nd.get("image", ""),
                relationships=nd.get("relationships", {}),
            )
        else:
            node = TextNode(
                id_=nd["id_"],
                text=nd.get("text", ""),
                metadata=nd["metadata"],
                relationships=nd.get("relationships", {}),
            )

        nodes.append(node)

    return nodes


def embed_nodes(nodes):
    text_embedding_model = create_embedding_model(1536)
    image_embedding_model = create_embedding_model(1024)

    for node in nodes:
        if isinstance(node, ImageNode):
            node.embedding = image_embedding_model.get_text_embedding(node.image)
        elif isinstance(node, TextNode) and node.text.strip():
            node.embedding = text_embedding_model.get_text_embedding(node.text)

    return nodes


def main():
    nodes = load_nodes_from_json("input.json")
    embedded_nodes = embed_nodes(nodes)

    # json_output = output(embedded_nodes) # For outputting to file
    # with open("output.json", "w") as f:
    #     json.dump(json_output, f, indent=4)

if __name__ == "__main__":
    main()
