import json
from embed import Embedder
from ingest import ingest

file_path = "output/input.json"


# Embed then Upload to Weaviate
def main():
    with open(file_path, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    embedder = Embedder()
    nodes = embedder.add_metadata(data)
    nodes = embedder.add_embeddings(nodes)

    # # Save embedded nodes to JSON
    # with open("output/output.json", "w") as f:
    #     json.dump(nodes, f, indent=4)

    # Upload to Weaviate
    ingest(nodes)


if __name__ == "__main__":
    main()
