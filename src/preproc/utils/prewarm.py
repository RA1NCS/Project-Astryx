#!/usr/bin/env python3
"""
Prewarm script to download and cache Docling models during Docker build.
This runs during 'docker build' to ensure models are baked into the image.
"""

import os
import sys
import logging

# Configure logging for build process
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def prewarm_docling_models():
    """Download and cache Docling models during Docker build."""

    # Import the converter creation functions
    from processors.processor import (
        _create_simple_docling_converter,
        _create_docling_converter,
    )

    logger.info("Starting build-time model prewarming...")

    # Path to prewarmer PDF (relative to script location)
    prewarmer_path = os.path.join(os.path.dirname(__file__), "prewarmer.pdf")

    if not os.path.exists(prewarmer_path):
        logger.error(f"Prewarmer PDF not found at {prewarmer_path}")
        sys.exit(1)

    logger.info(f"Using prewarmer PDF: {prewarmer_path}")

    try:
        # Create and warm simple converter
        logger.info("Creating simple converter and downloading models...")
        simple_converter = _create_simple_docling_converter()
        simple_result = simple_converter.convert(prewarmer_path)
        logger.info("Simple converter models downloaded and cached")

        # Create and warm complex converter
        logger.info("Creating complex converter and downloading models...")
        complex_converter = _create_docling_converter(complex_mode=True)
        complex_result = complex_converter.convert(prewarmer_path)
        logger.info("Complex converter models downloaded and cached")

        logger.info("Build-time model prewarming completed successfully!")

    except Exception as e:
        logger.error(f"Failed to prewarm models during build: {e}")
        sys.exit(1)


if __name__ == "__main__":
    prewarm_docling_models()
