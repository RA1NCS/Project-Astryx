import os
import json
import re
import sys

sys.path.append("..")
from upload import get_client
import weaviate.classes as wvc


# Extract page and chunk info from chunk IDs for sorting only
def parse_chunk_id_for_sort(chunk_id):
    # Pattern: txt_XXXX_pYY_cZZ or img_XXXX_pYY_iZZ
    pattern = r"(txt|img)_(\d+)_p(\d+)_([ci])(\d+)"
    match = re.match(pattern, chunk_id)

    if match:
        content_type, doc_id, page_num, chunk_type, chunk_num = match.groups()
        # Return sort key: (page, chunk_type priority, chunk_num)
        # 'c' (text) comes before 'i' (image) in sorting
        type_priority = 0 if chunk_type == "c" else 1
        return (int(page_num), type_priority, int(chunk_num))
    return (999, 999, 999)  # Invalid chunks go to end


# Download all text chunks with relationships from Weaviate
def download_text_chunks(client, username):
    text_col = client.collections.get("TextChunk").with_tenant(username)

    results = text_col.query.fetch_objects(
        include_vector=False,
        limit=1000,
        return_references=wvc.query.QueryReference(link_on="hasImages"),
    )

    chunks = []
    for obj in results.objects:
        # Get all properties except embedding
        chunk_data = dict(obj.properties)
        chunk_data["uuid"] = str(obj.uuid)

        # Add relationships if they exist
        relationships = {}
        if hasattr(obj, "references") and obj.references:
            if "hasImages" in obj.references:
                relationships["hasImages"] = [
                    str(ref.uuid) for ref in obj.references["hasImages"].objects
                ]

        if relationships:
            chunk_data["relationships"] = relationships

        chunks.append(chunk_data)

    return chunks


# Download all image chunks with relationships from Weaviate
def download_image_chunks(client, username):
    image_col = client.collections.get("ImageChunk").with_tenant(username)

    results = image_col.query.fetch_objects(
        include_vector=False,
        limit=1000,
        return_references=wvc.query.QueryReference(link_on="belongsToText"),
    )

    chunks = []
    for obj in results.objects:
        # Get all properties except embedding
        chunk_data = dict(obj.properties)
        chunk_data["uuid"] = str(obj.uuid)

        # Add relationships if they exist
        relationships = {}
        if hasattr(obj, "references") and obj.references:
            if "belongsToText" in obj.references:
                relationships["belongsToText"] = [
                    str(ref.uuid) for ref in obj.references["belongsToText"].objects
                ]

        if relationships:
            chunk_data["relationships"] = relationships

        chunks.append(chunk_data)

    return chunks


# Save all chunks in document order to a single file
def save_all_chunks_ordered(text_chunks, image_chunks, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # Combine all chunks
    all_chunks = []

    for chunk in text_chunks:
        chunk["type"] = "text"
        all_chunks.append(chunk)

    for chunk in image_chunks:
        chunk["type"] = "image"
        all_chunks.append(chunk)

    # Sort by document order using chunk_id
    all_chunks.sort(key=lambda x: parse_chunk_id_for_sort(x.get("chunk_id", "")))

    # Save to single file with full node data
    output_file = os.path.join(output_dir, "nodes.json")
    with open(output_file, "w") as f:
        json.dump({"total_nodes": len(all_chunks), "nodes": all_chunks}, f, indent=2)

    print(
        f"Saved all {len(all_chunks)} nodes with relationships → all_nodes_with_relationships.json"
    )
    return len(all_chunks)


# Main download function
def download_all_chunks(username="dev", output_dir="downloaded_chunks"):
    client = get_client()

    try:
        print(f"Downloading full nodes with relationships for user: {username}")

        # Download all chunks with relationships
        text_chunks = download_text_chunks(client, username)
        image_chunks = download_image_chunks(client, username)

        print(f"Downloaded {len(text_chunks)} text nodes")
        print(f"Downloaded {len(image_chunks)} image nodes")

        if not text_chunks and not image_chunks:
            print("No nodes found in Weaviate")
            return

        # Save all nodes in order
        total_count = save_all_chunks_ordered(text_chunks, image_chunks, output_dir)

        print(f"\n📊 Summary:")
        print(f"   Total nodes: {total_count}")
        print(f"   Text nodes: {len(text_chunks)}")
        print(f"   Image nodes: {len(image_chunks)}")
        print(f"   Output: {output_dir}/all_nodes_with_relationships.json")

    finally:
        client.close()


if __name__ == "__main__":
    download_all_chunks()
