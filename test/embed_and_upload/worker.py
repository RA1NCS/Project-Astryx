import json
from embed import add_embeddings, add_metadata
from upload import ingest

file_path = "output/input.json"


# Main processing pipeline for embedding and uploading nodes to Weaviate
def main():
    with open(file_path, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    # Load raw nodes as dictionaries
    nodes = add_metadata(data)

    # Embed text and images (returns dictionaries with embeddings)
    nodes = add_embeddings(nodes)

    # Save embedded nodes to JSON
    with open("output/output.json", "w") as f:
        json.dump(nodes, f, indent=4)

    # Upload to Weaviate (username extracted from node metadata)
    ingest(nodes)


if __name__ == "__main__":
    main()
