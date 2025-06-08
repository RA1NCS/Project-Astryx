#!/usr/bin/env python3
"""
Container App Flow Test - Main Flow Only
"""

import os
import sys
import json
from pathlib import Path

# Add container app source to path
container_app_path = Path(__file__).parent.parent.parent / "src" / "preproc"
sys.path.insert(0, str(container_app_path))

from worker import process_file_content, generate_output_blob_paths
from processors.processor import (
    _create_docling_converter,
    _create_simple_docling_converter,
)
from utils.blob_storage import BlobStorageManager


def main():
    """Main container app flow test"""
    print("🧪 Container App Flow Test")

    # Initialize converters
    import worker

    worker.simple_converter = _create_simple_docling_converter()
    worker.complex_converter = _create_docling_converter(complex_mode=True)

    # Setup blob manager
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_manager = BlobStorageManager(connection_string)

    # Download blob
    test_url = (
        "https://astryxstoringest.blob.core.windows.net/raw/dev/text_images_1_ef23.pdf"
    )
    container, blob_path = blob_manager.extract_blob_path_from_url(test_url)
    file_data = blob_manager.download_blob(container, blob_path)

    # Process file
    file_name = "text_1_07f9.pdf"
    text_json, image_json = process_file_content(file_data, file_name)

    # Generate output paths
    text_path, image_path = generate_output_blob_paths(blob_path)

    # Save outputs
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/test_text.json", "w") as f:
        json.dump(text_json, f, indent=2)
    with open(f"{output_dir}/test_images.json", "w") as f:
        json.dump(image_json, f, indent=2)

    print(
        f"✅ Processed: {text_json.get('total_chunks')} text chunks, {image_json.get('total_images')} images"
    )
    print(f"📁 Output: {text_path}, {image_path}")


if __name__ == "__main__":
    main()
