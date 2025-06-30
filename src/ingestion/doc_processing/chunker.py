import json
import re
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class Chunk:
    content: str
    chunk_id: str
    source_page: int = None
    char_start: int = 0
    char_end: int = 0
    metadata: Dict[str, Any] = None


class DocumentChunker:
    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        self.chunk_size = chunk_size
        self.overlap = overlap

    # Analyze document content to determine complexity level and modality
    def _analyze_document_complexity(
        self, content: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        images = content.get("images", [])
        tables = content.get("tables", [])
        markdown_text = content.get("markdown", "")

        has_images = len(images) > 0
        has_tables = len(tables) > 0
        has_markdown_tables = self._has_markdown_tables(markdown_text)

        is_complex = has_images or has_tables or has_markdown_tables

        modality = ["text"]
        if has_images:
            modality.append("images")

        return is_complex, modality

    # Check if text contains markdown tables by counting table indicators
    def _has_markdown_tables(self, text: str) -> bool:
        if not text:
            return False

        lines = text.split("\n")
        table_indicators = 0

        for line in lines:
            line = line.strip()
            if "|" in line and line.count("|") >= 2:
                table_indicators += 1
            elif re.match(r"^[\s\-\|\:]+$", line) and "|" in line:
                table_indicators += 1

        return table_indicators >= 2

    # Determine MIME type from file extension
    def _determine_file_type(self, file_name: str) -> str:
        if not file_name or file_name == "unknown":
            return "application/octet-stream"

        ext = file_name.lower().split(".")[-1] if "." in file_name else ""

        mime_mapping = {
            "pdf": "application/pdf",
            "docx": "application/docx",
            "xlsx": "application/xlsx",
            "pptx": "application/pptx",
            "md": "text/markdown",
            "markdown": "text/markdown",
            "html": "text/html",
            "htm": "text/html",
            "txt": "text/plain",
            "csv": "text/csv",
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "tiff": "image/tiff",
            "tif": "image/tiff",
            "bmp": "image/bmp",
            "webp": "image/webp",
        }

        return mime_mapping.get(ext, "application/octet-stream")

    # Create standardized chunk metadata with consistent structure
    def _create_chunk_metadata(
        self,
        file_type: str,
        file_name: str,
        images: List = None,
        document_complex: bool = False,
        modality: List[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        images = images or []
        modality = modality or ["text"]
        has_chunk_tables = kwargs.get("has_tables", False)
        chunk_complex = len(images) > 0 or has_chunk_tables

        # Update modality based on actual chunk content
        chunk_modality = ["text"]
        if len(images) > 0:
            chunk_modality.append("images")

        metadata = {
            "source_type": file_type,
            "file_name": file_name,
            "images": images,
            "image_count": len(images),
            "chunk_complex": chunk_complex,
            "document_complex": document_complex,
            "modality": chunk_modality,
        }

        # Filter out timing and count fields from kwargs
        filtered_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ["total_chunks", "chunking_time"]
        }
        metadata.update(filtered_kwargs)
        return metadata

    # Create chunk object with consistent structure and metadata
    def _create_chunk(
        self,
        content: str,
        chunk_id: str,
        source_page: int = 1,
        char_start: int = 0,
        char_end: int = 0,
        metadata_params: Dict = None,
    ) -> Chunk:
        return Chunk(
            content=content.strip(),
            chunk_id=chunk_id,
            source_page=source_page,
            char_start=char_start,
            char_end=char_end,
            metadata=metadata_params or {},
        )

    # Process docling result into semantic chunks with intelligent image association
    def chunk_docling_result(self, docling_result: Dict[str, Any]) -> List[Chunk]:

        def _chunk_processor():
            chunks = []
            content = docling_result.get("content", {})

            file_name = docling_result.get("metadata", {}).get("file_name", "unknown")
            file_type = self._determine_file_type(file_name)

            if "markdown" in content:
                markdown_text = content.get("markdown", "")
                images = content.get("images", [])
                document_complex, modality = self._analyze_document_complexity(content)

                # Create lookup dictionary for efficient image matching
                image_lookup = {
                    img.get("image_id", f"img_{i}"): img for i, img in enumerate(images)
                }

                if markdown_text.strip():
                    header_chunks = self._split_by_headers(markdown_text)

                    if header_chunks:
                        current_pos = 0
                        for i, header_chunk in enumerate(header_chunks):
                            chunk_images = self._extract_images_for_chunk(
                                header_chunk, image_lookup
                            )
                            has_tables = self._has_markdown_tables(header_chunk)

                            if len(header_chunk) <= self.chunk_size:
                                metadata = self._create_chunk_metadata(
                                    file_type,
                                    file_name,
                                    chunk_images,
                                    document_complex,
                                    modality,
                                    has_tables=has_tables,
                                )
                                chunk = self._create_chunk(
                                    header_chunk,
                                    f"chunk_h_0_1_{i}",
                                    1,
                                    current_pos,
                                    current_pos + len(header_chunk),
                                    metadata,
                                )
                                chunks.append(chunk)
                                # Account for separator in position tracking
                                current_pos += len(header_chunk) + 1
                            else:
                                # Handle oversized header sections by further splitting
                                sub_chunks = self._split_text_into_chunks(
                                    header_chunk,
                                    1,
                                    file_type,
                                    f"h_{i}",
                                    image_lookup,
                                    document_complex,
                                    modality,
                                    file_name,
                                )
                                chunks.extend(sub_chunks)
                    else:
                        markdown_chunks = self._split_text_into_chunks(
                            markdown_text,
                            1,
                            file_type,
                            "",
                            image_lookup,
                            document_complex,
                            modality,
                            file_name,
                        )
                        chunks.extend(markdown_chunks)

                # Create dedicated chunks for images with OCR text or captions
                for i, image in enumerate(images):
                    image_content_parts = []

                    if image.get("caption"):
                        image_content_parts.append(f"Image Caption: {image['caption']}")
                    if image.get("ocr_text"):
                        image_content_parts.append(
                            f"Image Text (OCR): {image['ocr_text']}"
                        )

                    if image_content_parts:
                        image_content = "\n".join(image_content_parts)
                        metadata = self._create_chunk_metadata(
                            file_type,
                            file_name,
                            [image],
                            document_complex,
                            modality,
                            has_caption=bool(image.get("caption")),
                            has_ocr_text=bool(image.get("ocr_text")),
                            primary_image=True,
                        )
                        # Position image chunks after main text content
                        image_start_pos = len(markdown_text) + i * 100
                        chunk = self._create_chunk(
                            image_content,
                            f"chunk_img_0_1_{i}",
                            1,
                            image_start_pos,
                            image_start_pos + len(image_content),
                            metadata,
                        )
                        chunks.append(chunk)

            else:
                # Handle legacy page-based structure when markdown not available
                pages = content.get("pages", [])
                for page_data in pages:
                    page_num = page_data.get("page", 1)
                    page_text = page_data.get("text", "")

                    if page_text.strip():
                        page_chunks = self._split_text_into_chunks(
                            page_text,
                            page_num,
                            file_type,
                            "",
                            None,
                            False,
                            ["text"],
                            file_name,
                        )
                        chunks.extend(page_chunks)

            return chunks

        chunks = _chunk_processor()
        return chunks

    # Associate images with text chunks using placeholders and semantic matching
    def _extract_images_for_chunk(
        self, text_chunk: str, image_lookup: Dict[str, Dict]
    ) -> List[Dict]:
        chunk_images = []

        image_markers = re.findall(r"<!-- image -->", text_chunk)

        if len(image_markers) > 0:
            # Use placeholder count to determine image assignment
            available_images = list(image_lookup.values())
            images_to_include = available_images[: len(image_markers)]
            chunk_images.extend(images_to_include)
        else:
            # Fall back to content similarity matching
            for image_data in image_lookup.values():
                caption = image_data.get("caption", "").strip()
                ocr_text = image_data.get("ocr_text", "").strip()

                if caption and len(caption) > 10:
                    # Match caption words with chunk text
                    caption_words = [w for w in caption.lower().split() if len(w) > 3]
                    chunk_lower = text_chunk.lower()

                    if caption_words and len(caption_words) > 2:
                        matching_words = sum(
                            1 for word in caption_words if word in chunk_lower
                        )
                        # Require 70% word overlap for caption matching
                        if matching_words >= len(caption_words) * 0.7:
                            chunk_images.append(image_data)
                            break

                elif ocr_text and len(ocr_text) > 20:
                    # Match OCR text with chunk content
                    ocr_words = [w for w in ocr_text.lower().split() if len(w) > 4]
                    chunk_lower = text_chunk.lower()

                    if ocr_words and len(ocr_words) > 3:
                        matching_words = sum(
                            1 for word in ocr_words if word in chunk_lower
                        )
                        # Require 80% word overlap for OCR matching
                        if matching_words >= len(ocr_words) * 0.8:
                            chunk_images.append(image_data)
                            break

        return chunk_images

    # Split text into logical sections based on markdown headers and document patterns
    def _split_by_headers(self, text: str) -> List[str]:
        lines = text.split("\n")
        sections = []
        current_section = []

        for line in lines:
            line_stripped = line.strip()

            # Detect various header patterns including markdown and structural headers
            is_header = re.match(r"^#{1,6}\s+.+", line_stripped) or (
                len(line_stripped) > 0
                and len(line_stripped) < 100
                and not line_stripped.endswith(".")
                and not line_stripped.endswith(",")
                and not line_stripped.startswith("-")
                and not line_stripped.startswith("*")
                and (
                    line_stripped.isupper()
                    or re.match(r"^[A-Z][^.]*[:\(]", line_stripped)
                    or re.match(r"^## .+", line_stripped)
                )
            )

            if is_header:
                if current_section:
                    sections.append("\n".join(current_section))
                current_section = [line]
            else:
                current_section.append(line)

        if current_section:
            sections.append("\n".join(current_section))

        # Filter out very short sections and HTML comments
        filtered_sections = []
        for section in sections:
            section_clean = section.strip()
            if len(section_clean) > 50 and not section_clean.startswith("<!--"):
                filtered_sections.append(section_clean)

        return filtered_sections

    # Split text into overlapping chunks respecting sentence boundaries
    def _split_text_into_chunks(
        self,
        text: str,
        page_num: int = 1,
        file_type: str = "text/plain",
        chunk_prefix: str = "",
        image_lookup: Dict[str, Dict] = None,
        document_complex: bool = False,
        modality: List[str] = None,
        file_name: str = "unknown",
    ) -> List[Chunk]:
        chunks = []
        text = text.strip()
        if not text:
            return chunks

        modality = modality or ["text"]

        sentences = self._split_into_sentences(text)

        current_chunk = ""
        current_start = 0
        chunk_count = 0

        for sentence in sentences:
            # Check if adding sentence would exceed chunk size limit
            if len(current_chunk) + len(sentence) > self.chunk_size and current_chunk:
                chunk_id = (
                    f"chunk_{chunk_prefix}_{page_num}_{chunk_count}"
                    if chunk_prefix
                    else f"chunk_{page_num}_{chunk_count}"
                )

                chunk_images = []
                if image_lookup:
                    chunk_images = self._extract_images_for_chunk(
                        current_chunk, image_lookup
                    )

                has_tables = self._has_markdown_tables(current_chunk)
                metadata = self._create_chunk_metadata(
                    file_type,
                    file_name,
                    chunk_images,
                    document_complex,
                    modality,
                    has_tables=has_tables,
                )

                chunk = self._create_chunk(
                    current_chunk,
                    chunk_id,
                    page_num,
                    current_start,
                    current_start + len(current_chunk),
                    metadata,
                )
                chunks.append(chunk)

                # Create overlap text for next chunk to maintain context
                overlap_text = ""
                if len(current_chunk) > self.overlap:
                    overlap_candidate = current_chunk[-self.overlap :]
                    space_pos = overlap_candidate.find(" ")
                    if space_pos > 0:
                        overlap_text = overlap_candidate[space_pos:].strip()
                    else:
                        overlap_text = overlap_candidate.strip()
                else:
                    overlap_text = current_chunk

                current_chunk = (
                    overlap_text + " " + sentence if overlap_text else sentence
                )
                current_start = (
                    current_start + len(current_chunk) - len(overlap_text)
                    if overlap_text
                    else current_start + len(current_chunk)
                )
                chunk_count += 1
            else:
                current_chunk += " " + sentence if current_chunk else sentence

        # Process remaining text as final chunk
        if current_chunk.strip():
            chunk_id = (
                f"chunk_{chunk_prefix}_{page_num}_{chunk_count}"
                if chunk_prefix
                else f"chunk_{page_num}_{chunk_count}"
            )

            chunk_images = []
            if image_lookup:
                chunk_images = self._extract_images_for_chunk(
                    current_chunk, image_lookup
                )

            has_tables = self._has_markdown_tables(current_chunk)
            metadata = self._create_chunk_metadata(
                file_type,
                file_name,
                chunk_images,
                document_complex,
                modality,
                has_tables=has_tables,
            )

            chunk = self._create_chunk(
                current_chunk,
                chunk_id,
                page_num,
                current_start,
                current_start + len(current_chunk),
                metadata,
            )
            chunks.append(chunk)

        return chunks

    # Split text into sentences using multiple strategies for robustness
    def _split_into_sentences(self, text: str) -> List[str]:
        sentence_endings = r"[.!?]+(?:\s|$)"
        sentences = re.split(sentence_endings, text)
        sentences = [s.strip() for s in sentences if s.strip()]

        # Fall back to line-based splitting if sentence detection fails
        if len(sentences) <= 1:
            sentences = [line.strip() for line in text.split("\n") if line.strip()]

        # Final fallback to period-based splitting
        if len(sentences) <= 1:
            sentences = [s.strip() + "." for s in text.split(".") if s.strip()]

        return sentences

    # Process docling result preserving page boundaries for text chunks
    def chunk_docling_result_with_pages(
        self, docling_result: Dict[str, Any], page_markdown: Dict[int, str] = None
    ) -> List[Chunk]:
        chunks = []
        content = docling_result.get("content", {})

        file_name = docling_result.get("metadata", {}).get("file_name", "unknown")
        file_type = self._determine_file_type(file_name)

        # Get images and complexity info
        images = content.get("images", [])
        document_complex, modality = self._analyze_document_complexity(content)

        # Create lookup dictionary for efficient image matching
        image_lookup = {
            img.get("image_id", f"img_{i}"): img for i, img in enumerate(images)
        }

        # Process page-by-page if page_markdown is provided
        if page_markdown:
            for page_num, page_text in page_markdown.items():
                if not page_text.strip():
                    continue

                # Adjust page number to be 1-indexed
                page_number = page_num + 1

                # Split page text into chunks while preserving page info
                page_chunks = self._split_text_into_chunks(
                    page_text,
                    page_number,  # Use actual page number
                    file_type,
                    f"p{page_number}",  # Page prefix
                    image_lookup,
                    document_complex,
                    modality,
                    file_name,
                )
                chunks.extend(page_chunks)

            # Process images separately (they already have page numbers)
            for i, image in enumerate(images):
                image_content_parts = []

                if image.get("caption"):
                    image_content_parts.append(f"Image Caption: {image['caption']}")
                if image.get("ocr_text"):
                    image_content_parts.append(f"Image Text (OCR): {image['ocr_text']}")

                if image_content_parts:
                    image_content = "\n".join(image_content_parts)
                    image_page = image.get("page_number", 1)

                    metadata = self._create_chunk_metadata(
                        file_type,
                        file_name,
                        [image],
                        document_complex,
                        modality,
                        has_caption=bool(image.get("caption")),
                        has_ocr_text=bool(image.get("ocr_text")),
                        primary_image=True,
                    )

                    # Use image's actual page number
                    chunk = self._create_chunk(
                        image_content,
                        f"chunk_img_{image_page}_{i}",
                        image_page,
                        0,  # Image chunks don't have meaningful char positions
                        len(image_content),
                        metadata,
                    )
                    chunks.append(chunk)

        else:
            # Fallback to original processing if no page_markdown provided
            return self.chunk_docling_result(docling_result)

        return chunks


# Create complete nodes document with file metadata
def create_nodes_document(
    chunks: List[Chunk],
    file_name: str = None,
    file_sha256: str = None,
    user: str = None,
) -> Dict[str, Any]:
    source_mime = "application/octet-stream"
    if file_name:
        chunker = DocumentChunker()
        source_mime = chunker._determine_file_type(file_name)

    nodes_doc = extract_nodes(chunks, file_sha256, user, source_mime)

    return nodes_doc


# Extract unified nodes list compatible with LlamaIndex v0.12.x TextNode/ImageNode schema
def extract_nodes(
    chunks: List[Chunk],
    file_sha256: str = None,
    user: str = None,
    source_mime: str = None,
) -> Dict[str, Any]:
    nodes = []
    text_counter = 0
    image_counter = 0

    # Extract last 4 characters of SHA256 for collision-resistant ID scheme
    sha4 = file_sha256[-4:] if file_sha256 and len(file_sha256) >= 4 else "0000"

    for chunk in chunks:
        chunk_metadata = chunk.metadata or {}
        chunk_images = chunk_metadata.get("images", [])

        text_counter += 1
        node_id = f"txt_{sha4}_p{chunk.source_page:02d}_c{text_counter:02d}"

        # Pre-calculate image node IDs for cross-referencing
        image_refs = []
        chunk_image_start = image_counter + 1
        for i, image in enumerate(chunk_images):
            # Use the image's own page_number for proper page assignment
            image_page = image.get("page_number", 1)
            image_refs.append(
                f"img_{sha4}_p{image_page:02d}_i{chunk_image_start + i:02d}"
            )

        text_node = {
            "id_": node_id,
            "type": "text",
            "text": chunk.content,
            "metadata": {
                "file_name": chunk_metadata.get("file_name", ""),
                "char_start": chunk.char_start,
                "char_len": len(chunk.content),
                "is_complex": chunk_metadata.get("document_complex", False),
                "has_tables": chunk_metadata.get("has_tables", False),
                "image_refs": image_refs,
                # Include page number only if chunk has valid page info (from page-aware chunking)
                **(
                    {"page": chunk.source_page}
                    if chunk.source_page and chunk.source_page > 0
                    else {}
                ),
            },
            "relationships": {},
            "embedding": None,
        }
        nodes.append(text_node)

        # Create image nodes with proper relationships to parent text
        for i, image in enumerate(chunk_images):
            image_counter += 1
            # Use the image's own page_number for proper page assignment
            image_page = image.get("page_number", 1)
            image_node_id = f"img_{sha4}_p{image_page:02d}_i{image_counter:02d}"

            image_node = {
                "id_": image_node_id,
                "type": "image",
                "image": image.get("base64", ""),
                "metadata": {
                    "file_name": chunk_metadata.get("file_name", ""),
                    "page": image_page,
                },
                "relationships": {"1": {"node_id": node_id, "node_type": "1"}},
                "embedding": None,
            }
            nodes.append(image_node)

    return {
        "file_sha256": file_sha256,
        "user": user,
        "source_mime": source_mime,
        "total_nodes": len(nodes),
        "nodes": nodes,
    }
