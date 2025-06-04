import os
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from typing import Dict, Optional


class BlobStorageManager:

    def __init__(self, connection_string: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.raw_container = "raw"
        self.processed_container = "processed"
        self._ensure_containers_exist()

    def _ensure_containers_exist(self):
        try:
            self.blob_service_client.create_container(self.raw_container)
        except Exception:
            pass

        try:
            self.blob_service_client.create_container(self.processed_container)
        except Exception:
            pass

    def upload_raw_file(
        self, blob_name: str, file_data: bytes, metadata: dict = None, tags: dict = None
    ) -> str:
        container_client = self.blob_service_client.get_container_client(
            self.raw_container
        )

        blob_client = container_client.upload_blob(
            name=blob_name,
            data=file_data,
            metadata=metadata,
            tags=tags,
            overwrite=False,  # Don't overwrite to detect duplicates
        )

        return blob_client.url

    def get_blob_url(self, container_name: str, blob_name: str) -> str:
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        return blob_client.url
