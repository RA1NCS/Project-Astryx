import base64
import os
from azure.storage.blob import BlobServiceClient
from typing import Dict, Tuple, List
from multiprocessing.dummy import Pool as ThreadPool


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

    # Upload base64 image to Azure blob storage and return public URL (used internally by batch function)
    def upload_image(self, tenant, image_base64, image_id):
        if not image_base64:
            raise Exception("No image found to upload")

        try:
            data = base64.b64decode(image_base64)
            blob_name = f"{tenant}/images/{image_id}.png"
            return self.upload_processed_file(
                blob_name, data, tags={"status": "indexed"}, content_type="image/png"
            )

        except Exception as e:
            raise Exception(f"Failed to upload image: {e}")

    # Upload multiple images concurrently using threading
    def upload_images_batch(self, upload_images: List[Dict]) -> List[str]:
        def upload_single_image(image):
            return self.upload_image(
                image["tenant"], image["image_base64"], image["image_id"]
            )

        with ThreadPool(8) as pool:
            return pool.map(upload_single_image, upload_images)

    def upload_processed_file(
        self,
        blob_name: str,
        file_data: bytes,
        metadata: dict = None,
        tags: dict = None,
        content_type: str = None,
    ) -> str:
        container_client = self.blob_service_client.get_container_client(
            self.processed_container
        )

        # Prepare content settings if content_type is provided
        content_settings = None
        if content_type:
            from azure.storage.blob import ContentSettings

            content_settings = ContentSettings(content_type=content_type)

        blob_client = container_client.upload_blob(
            name=blob_name,
            data=file_data,
            metadata=metadata,
            tags=tags,
            content_settings=content_settings,
            overwrite=True,
        )

        return blob_client.url

    def download_blob(self, container_name: str, blob_name: str) -> bytes:
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        return blob_client.download_blob().readall()

    def get_blob_properties(
        self, container_name: str, blob_name: str
    ) -> Tuple[Dict, Dict]:
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        properties = blob_client.get_blob_properties()
        return properties.metadata or {}, properties.tags or {}

    def update_blob_tags(
        self, container_name: str, blob_name: str, tags: Dict[str, str]
    ):
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        blob_client.set_blob_tags(tags)

    def extract_blob_path_from_url(self, blob_url: str) -> Tuple[str, str]:
        url_parts = blob_url.split("/")
        try:
            domain_index = next(
                i for i, part in enumerate(url_parts) if "blob.core.windows.net" in part
            )
            container = url_parts[domain_index + 1]
            blob_path = "/".join(url_parts[domain_index + 2 :])
            return container, blob_path
        except (StopIteration, IndexError):
            container = url_parts[-2] if url_parts[-2] != "blobs" else url_parts[-3]
            blob_path = "/".join(url_parts[url_parts.index(container) + 1 :])
            return container, blob_path
