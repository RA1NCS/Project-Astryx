import os
import sys
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from utils.blob_storage import BlobStorageManager
from utils.file_utils import compute_file_hash, get_mime_type, generate_blob_path

import uvicorn

load_dotenv()

app = FastAPI()

# Configure CORS to allow file uploads from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure Azure connection string is available before initializing services
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")

blob_manager = BlobStorageManager(connection_string)


# Handle file upload with deduplication based on SHA256 hash
@app.post("/upload")
async def upload_file(file: UploadFile = File(...), username: str = Form("dev")):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file selected")

    try:
        file_data = await file.read()

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

            return JSONResponse(
                content={
                    "status": "success",
                    "blob_url": blob_url,
                    "sha4": file_hash[-4:],
                    "sha256": file_hash,
                    "mime_type": mime_type,
                    "blob_path": blob_path,
                },
                status_code=202,
            )

        except Exception as e:
            # Check if this is a duplicate file error
            if "BlobAlreadyExists" in str(e):
                existing_blob_url = blob_manager.get_blob_url("raw", blob_path)
                return JSONResponse(
                    content={
                        "status": "duplicate",
                        "message": "Duplicate file",
                        "blob_url": existing_blob_url,
                        "sha4": file_hash[-4:],
                    },
                    status_code=409,
                )
            else:
                # Re-raise unexpected exceptions for proper error handling
                raise e

    except Exception as e:
        # Convert any unhandled exceptions to HTTP responses
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# Provide basic health check endpoint for service monitoring
@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
