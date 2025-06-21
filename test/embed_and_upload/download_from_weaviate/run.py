#!/usr/bin/env python3

from downloader import download_all_chunks
from verify_order import verify_nodes_order

if __name__ == "__main__":
    # Download all chunks and organize them
    download_all_chunks(username="dev", output_dir="downloaded_chunks")
    verify_nodes_order()
