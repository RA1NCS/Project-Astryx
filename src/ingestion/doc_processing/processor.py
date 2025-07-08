import os
import time
import warnings
import base64
import io
import traceback
from typing import List, Dict, Tuple, Optional
from io import BytesIO
import fitz

# Docling imports
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    AcceleratorOptions,
    AcceleratorDevice,
)
from docling.document_converter import PdfFormatOption
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend

warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="docling")

# Constants
THREAD_POOL_WORKERS = 16
DEFAULT_FALLBACK_CONTENT = "Content processed by Docling"
ERROR_FALLBACK_CONTENT = "Error processing page"
PAGE_SEPARATOR = "\n---\n"

# Type aliases
ProcessingResult = Tuple[Optional[object], Dict[int, str], int]
PageTriageResult = Tuple[List[int], List[int], List[str]]


# Factory function for creating DocumentConverter with appropriate pipeline
def _create_docling_converter(complex_mode: bool = False) -> DocumentConverter:
    pipeline_options = PdfPipelineOptions(
        do_table_structure=complex_mode,
        do_ocr=False,
        images_scale=1.0,
        generate_page_images=False,
        generate_picture_images=complex_mode,
        accelerator_options=AcceleratorOptions(
            num_threads=max(4, THREAD_POOL_WORKERS // 2),
            device=AcceleratorDevice.AUTO,
        ),
    )

    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options, backend=PyPdfiumDocumentBackend
            )
        }
    )


# Simple docling converter for non-PDF files (uses default pipeline)
def _create_simple_docling_converter() -> DocumentConverter:
    return DocumentConverter()


# Centralized error handling with logging and fallback
def _handle_processing_error(error: Exception, context: str, fallback_data: any) -> any:
    print(f"ERROR: {context} failed: {error}")
    traceback.print_exc()
    return fallback_data


# Generate fallback content for failed pages
def _create_fallback_page_content(
    page_nums: List[int], error_msg: str
) -> Dict[int, str]:
    return {
        page_num: f"# Page {page_num + 1}\n\n[{error_msg}]" for page_num in page_nums
    }


# Extract page number from provenance data
def _get_page_number_from_provenance(prov_list: List, default: int = 1) -> int:
    if prov_list:
        for prov in prov_list:
            if hasattr(prov, "page_no"):
                return prov.page_no
    return default


# Extract per-page markdown from Docling result
def _extract_page_markdown_from_result(
    docling_result: object, total_pages: int, mode_name: str
) -> Dict[int, str]:
    page_markdown = {}

    if not docling_result or not docling_result.document:
        return _create_fallback_page_content(
            list(range(total_pages)), f"Error: {mode_name} processing failed"
        )

    document = docling_result.document
    full_markdown = document.export_to_markdown()

    # Try to extract per-page content
    if hasattr(document, "pages") and document.pages:
        for i, page in enumerate(document.pages):
            try:
                page_content = (
                    page.export_to_markdown()
                    if hasattr(page, "export_to_markdown")
                    else ""
                )
                if not page_content.strip():
                    # Try to use portion of full markdown instead of fallback content
                    if full_markdown.strip():
                        lines = full_markdown.split("\n")
                        lines_per_page = max(1, len(lines) // total_pages)
                        start_idx = i * lines_per_page
                        end_idx = min((i + 1) * lines_per_page, len(lines))
                        page_content = "\n".join(lines[start_idx:end_idx]).strip()

                    # Only use fallback if we still have no content
                    if not page_content.strip():
                        page_content = ""  # Use empty content instead of debug text
                page_markdown[i] = page_content
            except Exception as e:
                print(f"Error extracting page {i + 1}: {e}")
                page_markdown[i] = f"# Page {i + 1}\n\n[{ERROR_FALLBACK_CONTENT}]"
    else:
        # Fallback: split the full markdown roughly by page count
        lines = full_markdown.split("\n")
        lines_per_page = max(1, len(lines) // total_pages)
        for i in range(total_pages):
            start_idx = i * lines_per_page
            end_idx = min((i + 1) * lines_per_page, len(lines))
            page_content = "\n".join(lines[start_idx:end_idx])
            if not page_content.strip():
                page_content = ""  # Use empty content instead of debug text
            page_markdown[i] = page_content

    return page_markdown


# Convert picture object to Base64 string
def _convert_image_to_base64(picture: object, document: object) -> Optional[str]:
    img_base64 = None

    # Try get_image with document parameter
    if hasattr(picture, "get_image"):
        try:
            pil_image = picture.get_image(document)
            if pil_image:
                buffer = io.BytesIO()
                pil_image.save(buffer, format="PNG")
                img_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
        except Exception as e:
            print(f"Error with get_image(document): {e}")

    # Try accessing image attribute directly
    if not img_base64 and hasattr(picture, "image") and picture.image:
        try:
            if hasattr(picture.image, "save"):  # PIL Image
                buffer = io.BytesIO()
                picture.image.save(buffer, format="PNG")
                img_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
            elif hasattr(picture.image, "data"):  # Raw data
                img_base64 = base64.b64encode(picture.image.data).decode("utf-8")
        except Exception as e:
            print(f"Error with direct image access: {e}")

    return img_base64


# Create standardized image metadata
def _create_image_metadata(img_base64: str, page_num: int, image_idx: int) -> Dict:
    return {
        "image_id": f"image_{image_idx}",
        "page_number": page_num,
        "base64": img_base64,
        "format": "PNG",
    }


# Extract single image with error handling
def _extract_single_image(
    picture: object, document: object, page_offset: int, img_idx: int
) -> Optional[Dict]:
    try:
        # Get page number from provenance
        page_num = page_offset
        if hasattr(picture, "prov") and picture.prov:
            page_num = (
                _get_page_number_from_provenance(picture.prov, page_offset)
                + page_offset
                - 1
            )

        img_base64 = _convert_image_to_base64(picture, document)

        if img_base64:
            return _create_image_metadata(img_base64, page_num, img_idx)
        else:
            return None
    except Exception as e:
        print(f"Error extracting image {img_idx}: {e}")
        return None


# Combined complexity detection and page triaging
def analyze_and_triage_pages(
    docling_result: object, total_pages: int
) -> PageTriageResult:
    simple_pages = []
    complex_pages = []

    print(f"Analyzing {total_pages} pages for content complexity...")

    # Detect complexity
    if not docling_result or not docling_result.document:
        complexity_info = ["simple"] * total_pages
    else:
        document = docling_result.document
        complexity_info = ["simple"] * total_pages

        try:
            # Check tables collection
            if hasattr(document, "tables") and document.tables:
                for table in document.tables:
                    page_num = _get_page_number_from_provenance(
                        getattr(table, "prov", None), 1
                    )
                    page_idx = max(0, min(page_num - 1, total_pages - 1))
                    if complexity_info[page_idx] == "simple":
                        complexity_info[page_idx] = "table"
                    elif complexity_info[page_idx] == "image":
                        complexity_info[page_idx] = "table+image"

            # Check pictures collection
            if hasattr(document, "pictures") and document.pictures:
                for picture in document.pictures:
                    page_num = _get_page_number_from_provenance(
                        getattr(picture, "prov", None), 1
                    )
                    page_idx = max(0, min(page_num - 1, total_pages - 1))
                    if complexity_info[page_idx] == "simple":
                        complexity_info[page_idx] = "image"
                    elif complexity_info[page_idx] == "table":
                        complexity_info[page_idx] = "table+image"
        except Exception as e:
            print(f"ERROR: Content analysis failed: {e}")
            complexity_info = ["simple"] * total_pages

    # Triage pages
    for page_num in range(total_pages):
        page_info = (
            complexity_info[page_num] if page_num < len(complexity_info) else "simple"
        )

        if page_info != "simple":
            complex_pages.append(page_num)
        else:
            simple_pages.append(page_num)

    return simple_pages, complex_pages, complexity_info


# Extract individual pages to in-memory PDF streams
def extract_pages_to_memory(
    file_path: str, page_numbers: List[int]
) -> List[Tuple[int, DocumentStream]]:
    page_streams = []

    try:
        source_doc = fitz.open(file_path)

        for page_num in page_numbers:
            if page_num < len(source_doc):
                # Create new document with single page
                single_page_doc = fitz.open()
                single_page_doc.insert_pdf(
                    source_doc, from_page=page_num, to_page=page_num
                )

                # Save to memory buffer
                pdf_bytes = BytesIO()
                single_page_doc.save(pdf_bytes)
                pdf_bytes.seek(0)

                # Create DocumentStream
                stream = DocumentStream(
                    name=f"page_{page_num + 1}.pdf", stream=pdf_bytes
                )
                page_streams.append((page_num, stream))

                single_page_doc.close()

        source_doc.close()
        return page_streams

    except Exception as e:
        return _handle_processing_error(e, "Error extracting pages to memory", [])


# Process DocumentStreams with Docling
def _process_page_streams_with_docling(
    streams: List, complex_mode: bool, converter=None
) -> List:
    if converter is None:
        converter = _create_docling_converter(complex_mode)
    return list(converter.convert_all(streams, raises_on_error=False))


# Extract images from Docling result and convert to Base64
def extract_images_from_result(
    docling_result: object, page_offset: int = 1
) -> List[Dict]:
    images = []

    try:
        if not docling_result or not docling_result.document:
            return images

        document = docling_result.document

        if hasattr(document, "pictures") and document.pictures:
            for i, picture in enumerate(document.pictures):
                extracted_image = _extract_single_image(
                    picture, document, page_offset, len(images) + 1
                )
                if extracted_image:
                    images.append(extracted_image)
    except Exception as e:
        print(f"Error in image extraction: {e}")

    return images


# Process entire document
def process_document_easy_mode(file_path: str, converter=None) -> ProcessingResult:
    with fitz.open(file_path) as doc:
        total_pages = len(doc)

    if converter is None:
        converter = _create_docling_converter(complex_mode=False)

    try:
        print(f"Processing document ({total_pages} pages)...")
        result = converter.convert(file_path)
        page_markdown = _extract_page_markdown_from_result(result, total_pages, "easy")
        return result, page_markdown, total_pages

    except Exception as e:
        fallback_markdown = _create_fallback_page_content(
            list(range(total_pages)), "Error: Easy mode processing failed"
        )
        return _handle_processing_error(
            e, "Document processing", (None, fallback_markdown, total_pages)
        )


# Process complex pages using Docling complex mode with full table structure
def process_complex_pages(
    file_path: str, page_numbers: List[int], converter=None
) -> Tuple[Dict[int, str], List[Dict]]:
    if not page_numbers:
        return {}, []

    try:
        print(
            f"Found {len(page_numbers)} complex pages, processing with enhanced mode..."
        )
        page_streams = extract_pages_to_memory(file_path, page_numbers)

        if not page_streams:
            return (
                _create_fallback_page_content(
                    page_numbers, "Error: Failed to extract page"
                ),
                [],
            )

        # Process streams with complex mode
        streams = [stream for _, stream in page_streams]
        conv_results = _process_page_streams_with_docling(
            streams, complex_mode=True, converter=converter
        )

        # Map results back to original page numbers and extract images
        page_markdown = {}
        all_images = []

        for i, (original_page_num, stream) in enumerate(page_streams):
            if i < len(conv_results) and conv_results[i].document:
                result = conv_results[i]
                markdown = result.document.export_to_markdown()
                page_markdown[original_page_num] = markdown if markdown.strip() else ""

                # Extract images from this result
                images_from_page = extract_images_from_result(
                    result, original_page_num + 1
                )
                all_images.extend(images_from_page)

            else:
                page_markdown[original_page_num] = (
                    f"# Page {original_page_num + 1}\n\n{ERROR_FALLBACK_CONTENT}\n"
                )
        return page_markdown, all_images

    except Exception as e:
        fallback_results = _create_fallback_page_content(
            page_numbers, f"Error processing page: {str(e)}"
        )
        return _handle_processing_error(
            e, "Enhanced processing", (fallback_results, [])
        )


# Merge easy and complex mode results
def _merge_processing_results(
    easy_results: Dict[int, str],
    complex_results: Dict[int, str],
    complex_pages: List[int],
) -> Dict[int, str]:
    final_page_markdown = easy_results.copy()

    # Override with complex mode results for complex pages
    for page_num in complex_pages:
        if page_num in complex_results:
            final_page_markdown[page_num] = complex_results[page_num]

    return final_page_markdown


# Generate standardized metadata
def _generate_processing_metadata(
    simple_pages: List[int],
    complex_pages: List[int],
    images: List[Dict],
    markdown: str,
    timings: Dict,
) -> Dict:
    # Generate table metadata
    tables = []
    table_count = 0
    for page_content in timings.get("complex_results", {}).values():
        if isinstance(page_content, str):
            table_count += page_content.count("| ")

    if table_count > 0:
        tables = [
            {"table_id": f"table_{i}"} for i in range(1, min(table_count // 10, 10) + 1)
        ]

    return {
        "page_count": len(simple_pages) + len(complex_pages),
        "fast_pages": len(simple_pages),
        "complex_pages": len(complex_pages),
        "images_found": len(images),
        "tables_found": len(tables),
        "total_text_length": len(markdown),
        "processing_time": timings["total_time"],
        "easy_mode_time": timings["easy_time"],
        "triage_time": timings["triage_time"],
        "complex_mode_time": timings["complex_time"],
        "merge_time": timings["merge_time"],
    }, tables


# Reassemble pages in correct order to single markdown blob
def merge_markdown_by_page(page_markdown: Dict[int, str], total_pages: int) -> str:
    merged_content = []

    for page_num in range(total_pages):
        if page_num in page_markdown:
            content = page_markdown[page_num].strip()
            if content:
                merged_content.append(content)
                merged_content.append(PAGE_SEPARATOR)

    return "\n".join(merged_content).strip()


# Process non-PDF files with simple docling conversion
def process_non_pdf_document(file_path: str, converter=None):
    """Process non-PDF documents using docling's default pipeline."""
    try:
        print(f"Processing document: {os.path.basename(file_path)}")

        if converter is None:
            converter = _create_simple_docling_converter()
        result = converter.convert(file_path)

        if not result or not result.document:
            print("ERROR: Document processing failed - no content returned")
            return None

        document = result.document
        markdown_content = document.export_to_markdown()

        # Extract images if any
        images = extract_images_from_result(result, page_offset=1)

        # Simple metadata for non-PDF documents
        metadata = {
            "page_count": 1,  # Non-PDF documents treated as single page
            "fast_pages": 1,
            "complex_pages": 0,
            "images_found": len(images),
            "tables_found": 0,  # Could be enhanced to count tables in markdown
            "total_text_length": len(markdown_content),
            "processing_time": 0,  # Will be set by caller
            "easy_mode_time": 0,
            "triage_time": 0,
            "complex_mode_time": 0,
            "merge_time": 0,
        }

        return {
            "type": "document",
            "content": {
                "markdown": markdown_content,
                "images": images,
                "tables": [],
                "metadata": metadata,
            },
            "metadata": {
                "file_name": os.path.basename(file_path),
                "file_size": os.path.getsize(file_path),
            },
        }

    except Exception as e:
        return _handle_processing_error(
            e, f"Error processing non-PDF document {file_path}", None
        )


# Check if file is PDF based on extension
def _is_pdf_file(file_path: str) -> bool:
    return file_path.lower().endswith(".pdf")


# Main processing function
def process_document(file_path: str, simple_converter=None, complex_converter=None):
    if not os.path.exists(file_path):
        return None

    start_time = time.time()

    try:
        # Route based on file type
        if _is_pdf_file(file_path):
            # PDF files: Use adaptive processing approach

            # Step 1: Initial document processing
            easy_start = time.time()
            easy_result, easy_page_markdown, total_pages = process_document_easy_mode(
                file_path, converter=simple_converter
            )
            easy_time = time.time() - easy_start

            # Step 2: Analyze content complexity
            triage_start = time.time()
            simple_pages, complex_pages, _ = analyze_and_triage_pages(
                easy_result, total_pages
            )
            triage_time = time.time() - triage_start

            # Step 3: Enhanced processing for complex pages
            complex_start = time.time()
            complex_results = {}
            all_images = []
            if complex_pages:
                complex_results, all_images = process_complex_pages(
                    file_path, complex_pages, converter=complex_converter
                )
            complex_time = time.time() - complex_start

            # Step 4: Merge results
            merge_start = time.time()
            final_page_markdown = _merge_processing_results(
                easy_page_markdown, complex_results, complex_pages
            )
            merged_markdown = merge_markdown_by_page(final_page_markdown, total_pages)
            merge_time = time.time() - merge_start

            total_time = time.time() - start_time

            # Generate metadata
            timings = {
                "total_time": total_time,
                "easy_time": easy_time,
                "triage_time": triage_time,
                "complex_time": complex_time,
                "merge_time": merge_time,
                "complex_results": complex_results,
            }

            metadata, tables = _generate_processing_metadata(
                simple_pages, complex_pages, all_images, merged_markdown, timings
            )

            print(f"Document processing completed: {total_pages} pages processed")

            return {
                "type": "document",
                "content": {
                    "markdown": merged_markdown,
                    "page_markdown": final_page_markdown,
                    "images": all_images,
                    "tables": tables,
                    "metadata": metadata,
                },
                "metadata": {
                    "file_name": os.path.basename(file_path),
                    "file_size": os.path.getsize(file_path),
                },
            }
        else:
            # Non-PDF files: Standard processing
            result = process_non_pdf_document(file_path, converter=simple_converter)

            if result:
                # Update processing time
                total_time = time.time() - start_time
                result["content"]["metadata"]["processing_time"] = total_time
                print(f"Document processing completed in {total_time:.2f}s")

            return result

    except Exception as e:
        return _handle_processing_error(e, "Error in processing", None)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        result = process_document(sys.argv[1])
        if result:
            meta = result["content"]["metadata"]
            print(
                f"Successfully processed {meta['page_count']} pages in {meta['processing_time']:.2f}s"
            )
            if meta.get("complex_pages", 0) > 0:
                print(f"  Enhanced processing applied to {meta['complex_pages']} pages")
            if meta.get("images_found", 0) > 0:
                print(f"  Extracted {meta['images_found']} images")
            if meta.get("tables_found", 0) > 0:
                print(f"  Found {meta['tables_found']} tables")
        else:
            print("Failed")
    else:
        print("Usage: python processor.py <file_path>")
