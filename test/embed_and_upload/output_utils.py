from llama_index.core.schema import TextNode, ImageNode


def output(nodes):
    output_data = {
        "nodes": [],
        "total_count": len(nodes),
        "text_count": 0,
        "image_count": 0,
    }

    for node in nodes:
        node_type = "image" if isinstance(node, ImageNode) else "text"

        if node_type == "image":
            output_data["image_count"] += 1
        else:
            output_data["text_count"] += 1

        node_data = {"id": node.id_, "type": node_type, "metadata": node.metadata}

        if hasattr(node, "embedding") and node.embedding is not None:
            if hasattr(node.embedding, "tolist"):
                node_data["embedding"] = node.embedding.tolist()
            else:
                node_data["embedding"] = node.embedding
        else:
            node_data["embedding"] = None

        if isinstance(node, ImageNode):
            node_data["image"] = node.image
        elif isinstance(node, TextNode):
            node_data["text"] = node.text

        output_data["nodes"].append(node_data)

    return output_data
