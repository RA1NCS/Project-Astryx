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
    from doc_processing.processor import (
        _create_simple_docling_converter,
        _create_docling_converter,
    )

    logger.info("Starting build-time model prewarming...")

    # Path to prewarmer PDF (relative to script location)
    prewarmer_path = os.path.join(os.path.dirname(__file__), "prewarmer.pdf")

    if not os.path.exists(prewarmer_path):
        logger.error("Prewarmer PDF not found at %s", prewarmer_path)
        sys.exit(1)

    logger.info("Using prewarmer PDF: %s", prewarmer_path)

    try:
        # Create and warm simple converter
        logger.info("Creating simple converter and downloading models...")
        simple_converter = _create_simple_docling_converter()
        simple_converter.convert(prewarmer_path)
        logger.info("Simple converter models downloaded and cached")

        # Create and warm complex converter
        logger.info("Creating complex converter and downloading models...")
        complex_converter = _create_docling_converter(complex_mode=True)
        complex_converter.convert(prewarmer_path)
        logger.info("Complex converter models downloaded and cached")

        logger.info("Build-time model prewarming completed successfully!")
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to prewarm models during build: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    prewarm_docling_models()
