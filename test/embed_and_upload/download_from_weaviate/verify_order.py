#!/usr/bin/env python3

import json


# Verify the downloaded nodes are in correct order with relationships
def verify_nodes_order():
    # Load the nodes file
    with open("downloaded_chunks/nodes.json", "r") as f:
        data = json.load(f)

    nodes = data["nodes"]

    print(f"📋 Node Order Verification with Relationships")
    print(f"Total nodes: {len(nodes)}")
    print(f"=" * 80)

    text_with_refs = 0
    images_with_refs = 0

    for i, node in enumerate(nodes):
        chunk_id = node["chunk_id"]
        node_type = node["type"]
        page = node.get("page", "?")

        # Check for relationships
        relationships = node.get("relationships", {})
        ref_info = ""
        if relationships:
            if node_type == "text" and "hasImages" in relationships:
                ref_count = len(relationships["hasImages"])
                ref_info = f" → {ref_count} images"
                text_with_refs += 1
            elif node_type == "image" and "belongsToText" in relationships:
                ref_count = len(relationships["belongsToText"])
                ref_info = f" → {ref_count} texts"
                images_with_refs += 1

        # Show preview
        if node_type == "text":
            text_content = node.get("text", "")
            preview = (
                text_content[:60] + "..." if len(text_content) > 60 else text_content
            )
        else:
            image_url = node.get("image_url", "No URL")
            preview = (
                f"Image: {image_url[-40:]}"
                if len(image_url) > 40
                else f"Image: {image_url}"
            )

        print(
            f"{i+1:2d}. {chunk_id:15s} | Page {page} | {node_type:5s}{ref_info:12s} | {preview}"
        )

    print(f"=" * 80)
    print(f"📊 Relationship Summary:")
    print(f"   Text nodes with image references: {text_with_refs}")
    print(f"   Image nodes with text references: {images_with_refs}")
    print(f"   Total nodes with relationships: {text_with_refs + images_with_refs}")

    # Show sample relationships
    print(f"\n🔗 Sample Relationships:")
    for i, node in enumerate(nodes[:5]):
        if "relationships" in node:
            chunk_id = node["chunk_id"]
            relationships = node["relationships"]
            for rel_type, rel_uuids in relationships.items():
                print(f"   {chunk_id} {rel_type}: {len(rel_uuids)} connections")
