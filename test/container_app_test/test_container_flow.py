#!/usr/bin/env python3
"""
Minimal Container App Flow Test - Unified Nodes Format
"""

import sys
import json
from pathlib import Path

# Add source paths
project_root = Path(__file__).parent.parent.parent
src_root = project_root / "src"
preproc_root = src_root / "preproc"

sys.path.insert(0, str(src_root))
sys.path.insert(0, str(preproc_root))

# Import functions
from worker import process_file_content, generate_output_blob_path
from doc_processing.processor import (
    _create_docling_converter,
    _create_simple_docling_converter,
)


def test_nodes_processing():
    """Test the new unified nodes processing flow"""
    print("🧪 Testing Unified Nodes Processing Flow")

    # Initialize converters (required by worker)
    import worker

    print("⚙️  Initializing converters...")
    worker.simple_converter = _create_simple_docling_converter()
    worker.complex_converter = _create_docling_converter(complex_mode=True)
    print("✅ Converters initialized")

    # Load test PDF
    test_pdf = Path(__file__).parent.parent / "datasets" / "custom"
    if not test_pdf.exists():
        print(f"❌ Test PDF not found at: {test_pdf}")
        return False

    # Process content using unified nodes format
    try:
        # Process content using unified nodes format
        print("📄 Processing test PDF...")
        # Find the actual PDF file in the custom directory
        pdf_file = test_pdf / "images_1.pdf"

        # Calculate SHA256 for testing
        file_data = pdf_file.read_bytes()
        from worker import calculate_sha256

        file_sha256 = calculate_sha256(file_data)

        print(f"📊 File SHA256: {file_sha256[:8]}...")

        nodes_doc = process_file_content(
            file_data, pdf_file.name, file_sha256, "test_user"
        )

        # Verify structure
        assert "nodes" in nodes_doc, "Missing nodes array"
        assert "total_nodes" in nodes_doc, "Missing total_nodes"
        assert "file_sha256" in nodes_doc, "Missing file_sha256"
        assert "user" in nodes_doc, "Missing user"

        # Verify SHA256 matches what we calculated
        assert (
            nodes_doc["file_sha256"] == file_sha256
        ), f"SHA256 mismatch: expected {file_sha256}, got {nodes_doc['file_sha256']}"

        print(f"✅ Generated {nodes_doc['total_nodes']} nodes")
        print(f"🏷️  Source MIME: {nodes_doc.get('source_mime', 'N/A')}")
        print(f"👤 User: {nodes_doc.get('user', 'N/A')}")
        print(f"🔑 File SHA256: {nodes_doc.get('file_sha256', 'N/A')[:8]}...")

        # Show first few nodes
        for i, node in enumerate(nodes_doc["nodes"][:3]):
            node_type = node.get("type", "unknown")
            node_id = node.get("id_", "no-id")
            if node_type == "text":
                text_preview = (
                    node.get("text", "")[:50] + "..."
                    if len(node.get("text", "")) > 50
                    else node.get("text", "")
                )
                print(f"  Node {i+1}: {node_id} (text) - {text_preview}")
            else:
                print(f"  Node {i+1}: {node_id} ({node_type})")

        if nodes_doc["total_nodes"] > 3:
            print(f"  ... and {nodes_doc['total_nodes'] - 3} more nodes")

        # Test output path generation
        test_blob_path = "dev/test_document.pdf"
        output_path = generate_output_blob_path(test_blob_path)
        print(f"📁 Output path: {output_path}")

        # Test username extraction
        from worker import extract_username_from_path

        test_paths = [
            "raw/dev/text_images_1.pdf",
            "raw/shreyan/document.pdf",
            "raw/alice/file.docx",
            "invalid/path/file.pdf",
        ]

        print("🧪 Testing username extraction:")
        for path in test_paths:
            user = extract_username_from_path(path)
            print(f"  {path} → {user}")

        # Save output for inspection
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        output_file = output_dir / "test_nodes.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(nodes_doc, f, indent=2, ensure_ascii=False)

        print(f"💾 Saved output to: {output_file}")
        print("🎉 Test completed successfully!")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run the test"""
    print("🚀 Container App Flow Test - Unified Nodes Format")
    print("=" * 50)

    success = test_nodes_processing()

    print("=" * 50)
    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
