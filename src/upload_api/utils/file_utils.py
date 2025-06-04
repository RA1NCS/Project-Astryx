import hashlib
import filetype


def compute_file_hash(file_data: bytes) -> str:
    """Compute SHA256 hash of file data."""
    return hashlib.sha256(file_data).hexdigest()


def get_mime_type(file_data: bytes) -> str:
    """Detect MIME type from file content."""
    kind = filetype.guess(file_data)
    if kind is not None:
        return kind.mime
    return "application/octet-stream"


def generate_blob_path(username: str, filename: str, file_hash: str) -> str:
    """Generate blob path using username, filename and hash."""
    sha4 = file_hash[-4:]
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename
    extension = filename.rsplit(".", 1)[1] if "." in filename else ""

    blob_name = f"{name_without_ext}_{sha4}"
    if extension:
        blob_name += f".{extension}"

    return f"{username}/{blob_name}"
