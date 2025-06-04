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

    def _analyze_document_complexity(
        self, content: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """Analyze document content to determine complexity level and modality."""
        images = content.get("images", [])
        tables = content.get("tables", [])
        markdown_text = content.get("markdown", "")

        has_images = len(images) > 0
        has_tables = len(tables) > 0
        has_markdown_tables = self._has_markdown_tables(markdown_text)

        # Determine complexity (boolean)
        is_complex = has_images or has_tables or has_markdown_tables

        # Determine modality
        modality = ["text"]
        if has_images:
            modality.append("images")

        return is_complex, modality

    def _has_markdown_tables(self, text: str) -> bool:
        """Check if text contains markdown tables."""
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

    def _determine_file_type(self, file_name: str) -> str:
        """Determine file type/mime type from file extension."""
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

    def _create_chunk_metadata(
        self,
        file_type: str,
        file_name: str,
        images: List = None,
        document_complex: bool = False,
        modality: List[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Factory function for creating consistent chunk metadata."""
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

        # Add other kwargs but exclude total_chunks and chunking_time
        filtered_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ["total_chunks", "chunking_time"]
        }
        metadata.update(filtered_kwargs)
        return metadata

    def _create_chunk(
        self,
        content: str,
        chunk_id: str,
        source_page: int = 1,
        char_start: int = 0,
        char_end: int = 0,
        metadata_params: Dict = None,
    ) -> Chunk:
        """Unified chunk creation with metadata factory."""
        return Chunk(
            content=content.strip(),
            chunk_id=chunk_id,
            source_page=source_page,
            char_start=char_start,
            char_end=char_end,
            metadata=metadata_params or {},
        )

    def chunk_docling_result(self, docling_result: Dict[str, Any]) -> List[Chunk]:
        """Process docling result into semantic chunks with image association."""

        def _chunk_processor():
            chunks = []
            content = docling_result.get("content", {})

            # Extract file information
            file_name = docling_result.get("metadata", {}).get("file_name", "unknown")
            file_type = self._determine_file_type(file_name)

            if "markdown" in content:
                markdown_text = content.get("markdown", "")
                images = content.get("images", [])
                document_complex, modality = self._analyze_document_complexity(content)

                # Create lookup for fast image matching
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
                                current_pos += len(header_chunk) + 1  # +1 for separator
                            else:
                                # Split oversized header sections
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

                # Create separate chunks for images with OCR content
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
                        # Use length of markdown text as starting position for image chunks
                        image_start_pos = (
                            len(markdown_text) + i * 100
                        )  # Approximate offset
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
                # Fallback to legacy structure
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

    def _extract_images_for_chunk(
        self, text_chunk: str, image_lookup: Dict[str, Dict]
    ) -> List[Dict]:
        """Associate images with text chunks based on placeholders and content matching."""
        chunk_images = []

        # Count image placeholders in this chunk
        image_markers = re.findall(r"<!-- image -->", text_chunk)

        if len(image_markers) > 0:
            # Assign images based on placeholder count
            available_images = list(image_lookup.values())
            images_to_include = available_images[: len(image_markers)]
            chunk_images.extend(images_to_include)
        else:
            # Match based on caption/OCR text similarity
            for image_data in image_lookup.values():
                caption = image_data.get("caption", "").strip()
                ocr_text = image_data.get("ocr_text", "").strip()

                if caption and len(caption) > 10:
                    # Check caption word overlap with chunk
                    caption_words = [w for w in caption.lower().split() if len(w) > 3]
                    chunk_lower = text_chunk.lower()

                    if caption_words and len(caption_words) > 2:
                        matching_words = sum(
                            1 for word in caption_words if word in chunk_lower
                        )
                        if matching_words >= len(caption_words) * 0.7:
                            chunk_images.append(image_data)
                            break

                elif ocr_text and len(ocr_text) > 20:
                    # Check OCR text overlap with chunk
                    ocr_words = [w for w in ocr_text.lower().split() if len(w) > 4]
                    chunk_lower = text_chunk.lower()

                    if ocr_words and len(ocr_words) > 3:
                        matching_words = sum(
                            1 for word in ocr_words if word in chunk_lower
                        )
                        if matching_words >= len(ocr_words) * 0.8:
                            chunk_images.append(image_data)
                            break

        return chunk_images

    def _split_by_headers(self, text: str) -> List[str]:
        """Split text by markdown headers and document structure patterns."""
        lines = text.split("\n")
        sections = []
        current_section = []

        for line in lines:
            line_stripped = line.strip()

            # Detect various header patterns
            is_header = (
                # Standard markdown headers
                re.match(r"^#{1,6}\s+.+", line_stripped)
                or (
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
            )

            if is_header:
                if current_section:
                    sections.append("\n".join(current_section))
                current_section = [line]
            else:
                current_section.append(line)

        if current_section:
            sections.append("\n".join(current_section))

        # Filter out short or comment sections
        filtered_sections = []
        for section in sections:
            section_clean = section.strip()
            if len(section_clean) > 50 and not section_clean.startswith("<!--"):
                filtered_sections.append(section_clean)

        return filtered_sections

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
        """Split text into chunks with overlap and sentence boundaries."""
        chunks = []
        text = text.strip()
        if not text:
            return chunks

        modality = modality or ["text"]

        # Split by sentences for coherent chunks
        sentences = self._split_into_sentences(text)

        current_chunk = ""
        current_start = 0
        chunk_count = 0

        for sentence in sentences:
            # Check if adding sentence exceeds chunk size
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

                # Start new chunk with overlap (inline _get_overlap_text logic)
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

        # Handle final chunk
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

    def _split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences with fallback strategies."""
        sentence_endings = r"[.!?]+(?:\s|$)"
        sentences = re.split(sentence_endings, text)
        sentences = [s.strip() for s in sentences if s.strip()]

        # Fallback to line splitting
        if len(sentences) <= 1:
            sentences = [line.strip() for line in text.split("\n") if line.strip()]

        # Fallback to period splitting
        if len(sentences) <= 1:
            sentences = [s.strip() + "." for s in text.split(".") if s.strip()]

        return sentences


def save_chunks(chunks: List[Chunk], output_path: str):
    """Save chunks to JSON file."""
    chunks_data = []
    for chunk in chunks:
        chunk_data = {
            "chunk_id": chunk.chunk_id,
            "content": chunk.content,
            "source_page": chunk.source_page,
            "char_start": chunk.char_start,
            "char_end": chunk.char_end,
            "char_length": len(chunk.content),
            "metadata": chunk.metadata or {},
        }
        chunks_data.append(chunk_data)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {"total_chunks": len(chunks_data), "chunks": chunks_data},
            f,
            indent=2,
            ensure_ascii=False,
        )


def extract_text_chunks(chunks: List[Chunk]) -> Dict[str, Any]:
    """Extract only text content and return as JSON data."""
    text_chunks_data = []

    for chunk in chunks:
        # Create text-specific metadata (exclude images)
        text_metadata = chunk.metadata.copy() if chunk.metadata else {}
        text_metadata.pop("images", None)
        text_metadata.pop("image_count", None)
        text_metadata["modality"] = ["text"]

        text_chunk_data = {
            "chunk_id": chunk.chunk_id,
            "text": chunk.content,
            "metadata": {
                "source_type": text_metadata.get("source_type", ""),
                "file_name": text_metadata.get("file_name", ""),
                "source_page": chunk.source_page,
                "char_start": chunk.char_start,
                "char_end": chunk.char_end,
                "char_length": len(chunk.content),
                "document_complex": text_metadata.get("document_complex", False),
                "modality": ["text"],
                "has_tables": text_metadata.get("has_tables", False),
            },
        }
        text_chunks_data.append(text_chunk_data)

    return {"total_chunks": len(text_chunks_data), "chunks": text_chunks_data}


def extract_image_chunks(chunks: List[Chunk]) -> Dict[str, Any]:
    """Extract only image data and return as JSON data."""
    image_chunks_data = []

    for chunk in chunks:
        chunk_metadata = chunk.metadata or {}
        images = chunk_metadata.get("images", [])

        # Only process chunks that have images
        if images:
            for image in images:
                image_chunk_data = {
                    "chunk_id": chunk.chunk_id,
                    "image_id": image.get("image_id", ""),
                    "page_number": image.get("page_number", chunk.source_page),
                    "image_data_base64": image.get("base64", ""),
                    "format": image.get("format", "PNG"),
                    "metadata": {
                        "source_type": chunk_metadata.get("source_type", ""),
                        "file_name": chunk_metadata.get("file_name", ""),
                        "source_page": image.get("page_number", chunk.source_page),
                        "char_start": chunk.char_start,
                        "char_end": chunk.char_end,
                        "char_length": len(chunk.content),
                        "document_complex": chunk_metadata.get(
                            "document_complex", False
                        ),
                        "modality": ["images"],
                        "has_tables": chunk_metadata.get("has_tables", False),
                    },
                }
                image_chunks_data.append(image_chunk_data)

    return {"total_images": len(image_chunks_data), "images": image_chunks_data}
