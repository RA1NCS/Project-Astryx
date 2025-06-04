import os
import sys
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

from utils.blob_storage import BlobStorageManager
from utils.file_utils import compute_file_hash, get_mime_type, generate_blob_path

load_dotenv()

app = Flask(__name__)
CORS(app)

# Initialize blob storage manager
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")

blob_manager = BlobStorageManager(connection_string)


@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    # if "username" not in request.form:
    #     return jsonify({"error": "Username parameter is required"}), 400

    file = request.files["file"]
    username = request.form.get("username", "dev")

    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    try:
        file_data = file.read()

        file_hash = compute_file_hash(file_data)
        mime_type = get_mime_type(file_data)
        blob_path = generate_blob_path(username, file.filename, file_hash)

        # Prepare metadata and tags for blob
        metadata = {
            "user_name": username,
            "sha256": file_hash,
            "file_name": file.filename,
        }

        tags = {"mime_type": mime_type}

        # Check for duplicate by attempting upload with overwrite=False
        try:
            blob_url = blob_manager.upload_raw_file(
                blob_name=blob_path, file_data=file_data, metadata=metadata, tags=tags
            )

            return (
                jsonify(
                    {
                        "status": "success",
                        "blob_url": blob_url,
                        "sha4": file_hash[-4:],
                        "sha256": file_hash,
                        "mime_type": mime_type,
                        "blob_path": blob_path,
                    }
                ),
                202,
            )

        except Exception as e:
            # Check if this is a duplicate file error
            if "BlobAlreadyExists" in str(e):
                existing_blob_url = blob_manager.get_blob_url("raw", blob_path)
                return (
                    jsonify(
                        {
                            "status": "duplicate",
                            "message": "Duplicate file",
                            "blob_url": existing_blob_url,
                            "sha4": file_hash[-4:],
                        }
                    ),
                    409,
                )
            else:
                raise e

    except Exception as e:
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(debug=False)
