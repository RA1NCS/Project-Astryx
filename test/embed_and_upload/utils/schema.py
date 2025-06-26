from weaviate.classes.config import DataType

COMMON_SCHEMA = {
    "file_name": DataType.TEXT,
    "page": DataType.INT,
    "file_sha256": DataType.TEXT,
    "user": DataType.TEXT,
    "source_mime": DataType.TEXT,
    "total_nodes": DataType.INT,
    "chunk_id": DataType.TEXT,
}

TEXT_SCHEMA = {
    **COMMON_SCHEMA,
    "text": DataType.TEXT,
    "char_start": DataType.INT,
    "char_len": DataType.INT,
    "is_complex": DataType.BOOL,
    "has_tables": DataType.BOOL,
    "image_refs": DataType.TEXT_ARRAY,
}

IMAGE_SCHEMA = {
    **COMMON_SCHEMA,
    "image_url": DataType.TEXT,
}
