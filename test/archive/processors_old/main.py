#!/usr/bin/env python3

import os
import sys
import json
import time

from processor import process_document
from chunker import (
    DocumentChunker,
    save_chunks,
    extract_text_chunks,
    extract_image_chunks,
)

# Constants
CHUNK_SIZE = 1000
OVERLAP = 200
OUTPUT_DIR = "output"

# Supported file extensions
SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".xlsx",
    ".pptx",
    ".md",
    ".markdown",
    ".adoc",
    ".html",
    ".xhtml",
    ".htm",
    ".csv",
    ".png",
    ".jpeg",
    ".jpg",
    ".tiff",
    ".tif",
    ".bmp",
    ".webp",
}


def is_supported_file(file_path: str) -> bool:
    """Check if file extension is supported."""
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_EXTENSIONS


def process(file_path: str):
    """Process document and return result."""
    if not os.path.exists(file_path):
        return None

    if not is_supported_file(file_path):
        print(f"Unsupported file type: {os.path.splitext(file_path)[1]}")
        return None

    return process_document(file_path)


def chunk(result):
    """Chunk processed document result."""
    if not result:
        return None

    chunker = DocumentChunker(chunk_size=CHUNK_SIZE, overlap=OVERLAP)
    return chunker.chunk_docling_result(result)


def display_statistics(result, chunks=None, processing_time=0):
    """Display processing statistics."""
    if not result:
        print("Processing failed")
        return

    meta = result["content"]["metadata"]
    file_name = result["metadata"]["file_name"]

    if chunks:
        print(
            f"Processed {file_name}: {meta['page_count']} pages, {len(chunks)} chunks in {processing_time:.2f}s"
        )
    else:
        print(
            f"Processed {file_name}: {meta['page_count']} pages in {processing_time:.2f}s"
        )


def save_results(
    file_path: str,
    text_json: dict,
    image_json: dict,
    unchunked_result=None,
    chunked_result=None,
    save_unchunked=False,
    saved_mixed=False,
):
    """Save processing results to output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Extract filename from path and create safe output name with extension
    name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    original_ext = os.path.splitext(file_path)[1]
    file_name = f"{name_without_ext}{original_ext.replace('.', '_')}"

    # Save unchunked results
    if save_unchunked:
        processed_file = f"{OUTPUT_DIR}/{file_name}_unchunked.json"

        with open(processed_file, "w", encoding="utf-8") as f:
            json.dump(unchunked_result, f, indent=2, ensure_ascii=False)

        print(f"Saved: {processed_file}")

    if saved_mixed:
        chunks_file = f"{OUTPUT_DIR}/{file_name}.json"
        save_chunks(chunked_result, chunks_file)
        print(f"Mixed chunks: {chunks_file}")

    # Save text JSON
    text_file = f"{OUTPUT_DIR}/{file_name}_text.json"
    with open(text_file, "w", encoding="utf-8") as f:
        json.dump(text_json, f, indent=2, ensure_ascii=False)

    # Save image JSON
    image_file = f"{OUTPUT_DIR}/{file_name}_images.json"
    with open(image_file, "w", encoding="utf-8") as f:
        json.dump(image_json, f, indent=2, ensure_ascii=False)

    print(f"[Saved] Text chunks: {text_file} | Image chunks: {image_file}")


def main():
    """Main processing function."""
    if len(sys.argv) < 2:
        print("Usage: python main.py <file_path>")
        return None, None

    file_path = sys.argv[1]

    # Process document
    start_time = time.time()
    unchunked_result = process(file_path)
    processing_time = time.time() - start_time

    if not unchunked_result:
        return None, None

    # Create chunks
    chunked_result = chunk(unchunked_result)
    text_json = extract_text_chunks(chunked_result)
    image_json = extract_image_chunks(chunked_result)

    total_time = time.time() - start_time

    # Display results
    display_statistics(unchunked_result, chunked_result, total_time)

    # Save results (optional mixed format)
    save_results(
        file_path,
        text_json,
        image_json,
        unchunked_result=unchunked_result,
        chunked_result=chunked_result,
        save_unchunked=False,
        saved_mixed=False,
    )

    return text_json, image_json


if __name__ == "__main__":
    print(json.dumps(main(), indent=2))
    exit(0)
