import logging
import json
import os
import tempfile
import time
import signal
import sys
import hashlib
from dotenv import load_dotenv
from azure.storage.queue import QueueServiceClient, BinaryBase64DecodePolicy
from llamaindex_embed.embed_nodes import add_metadata, add_embeddings
from weaviate_ingest.build_chunks import build_chunks
from utils.blob_utils import BlobStorageManager
from utils.client import get_client
from utils.collection import (
    create_collection,
    add_reference,
    get_collection_with_tenant,
    get_collection_name,
)
from weaviate_ingest.build_chunks import build_chunks
from utils.objects import batch_upload_objects, generate_uuid
from utils.schema import TEXT_SCHEMA, IMAGE_SCHEMA
from doc_processing.processor import (
    process_document,
    _create_docling_converter,
    _create_simple_docling_converter,
)
from doc_processing.chunker import (
    DocumentChunker,
    create_nodes_document,
)

# Reduce verbose logging from various libraries
logging.getLogger("azure.storage").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("docling").setLevel(logging.WARNING)
logging.getLogger("docling.document_converter").setLevel(logging.WARNING)
logging.getLogger("docling.backend").setLevel(logging.WARNING)
logging.getLogger("docling.datamodel").setLevel(logging.WARNING)

# Configure main logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

load_dotenv()

# Constants
CHUNK_SIZE = 1000
OVERLAP = 200
QUEUE_NAME = "ingest-jobs"
POLL_INTERVAL = 2
VISIBILITY_TIMEOUT = 180
MAX_MESSAGES = 1

# Initialize managers lazily to avoid startup errors
blob_manager = None
simple_converter = None
complex_converter = None


# Calculate SHA256 hash of file content
def calculate_sha256(file_data: bytes) -> str:
    return hashlib.sha256(file_data).hexdigest()


# Extract username from blob path pattern
def extract_username_from_path(blob_path: str) -> str:
    path_parts = blob_path.split("/")
    # Handle both "raw/dev/file.pdf" and "dev/file.pdf" patterns
    if len(path_parts) >= 2:
        if path_parts[0] == "raw":
            return path_parts[1]  # raw/dev/file.pdf -> dev
        else:
            return path_parts[0]  # dev/file.pdf -> dev
    return "unknown"


# Create TextChunk and ImageChunk collections with bidirectional references
def initialize_collections(client):
    existing_collections = set(client.collections.list_all().keys())

    # Create Collections
    create_collection(
        client,
        "TextChunk",
        TEXT_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )
    create_collection(
        client,
        "ImageChunk",
        IMAGE_SCHEMA,
        existing_collections,
        multi_tenancy=True,
        quantization=None,
    )

    # Add references only for newly created collections
    if (
        "TextChunk" not in existing_collections
        or "ImageChunk" not in existing_collections
    ):
        add_reference(client, "TextChunk", "hasImages", "ImageChunk")
        add_reference(client, "ImageChunk", "belongsToText", "TextChunk")


# Batch upload processed chunks to their respective collections
def batch_upload_chunks(client, chunks):
    for collection_name, data in chunks.items():
        # Convert DataObjects to format expected by batch upload utility
        objects_for_batch = []
        for obj in data["objs"]:
            objects_for_batch.append(
                {
                    "uuid": obj.uuid,
                    "properties": obj.properties,
                    "vector": obj.vector,
                }
            )

        # Delegate to batch upload function
        batch_upload_objects(
            client=client,
            collection_name=collection_name,
            objects=objects_for_batch,
            tenant_name=data["tenant"],
        )


# Extract reference UUIDs from chunk relationships or metadata
def get_reference_uuids(chunk):
    if chunk["type"] == "image":
        return [
            generate_uuid(rel["chunk_id"])
            for rel in chunk.get("relationships", {}).values()
            if isinstance(rel, dict) and "chunk_id" in rel
        ]
    return [generate_uuid(ref) for ref in chunk["metadata"].get("image_refs", [])]


# Establish cross-references between text and image chunks
def add_references(client, chunks):
    username = chunks[0]["metadata"]["user"]

    for chunk in chunks:
        ref_uuids = get_reference_uuids(chunk)
        if not ref_uuids:
            continue

        collection = get_collection_with_tenant(
            client, get_collection_name(chunk["type"]), username
        )
        ref_name = "belongsToText" if chunk["type"] == "image" else "hasImages"

        for target_uuid in ref_uuids:
            try:
                collection.data.reference_add(
                    from_uuid=generate_uuid(chunk["id_"]),
                    from_property=ref_name,
                    to=target_uuid,
                )
            except Exception:
                pass


# Process and upload chunks to Weaviate with complete cross-reference support
def ingest(chunks):
    if not chunks:
        raise ValueError("No chunks provided for ingestion")

    # Extract username from chunk metadata
    username = chunks[0]["metadata"].get("user")

    if not username:
        raise ValueError("No username found in chunk metadata")

    client = get_client()
    initialize_collections(client)
    built_chunks = build_chunks(chunks)
    batch_upload_chunks(client, built_chunks)
    add_references(client, chunks)

    client.close()


# Process file content using the unified nodes format
def process_file_content(
    file_data: bytes, file_name: str, file_sha256: str = None, user: str = None
):
    # Calculate SHA256 if not provided
    if not file_sha256:
        file_sha256 = calculate_sha256(file_data)

    # Create temporary file``
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(file_name)[1]
    ) as temp_file:
        temp_file.write(file_data)
        temp_file_path = temp_file.name

    try:
        # Process document using local processor with pre-warmed converters
        global simple_converter, complex_converter
        unchunked_result = process_document(
            temp_file_path, simple_converter, complex_converter
        )

        if not unchunked_result:
            raise Exception("Document processing failed")

        unchunked_result["metadata"]["file_name"] = file_name

        # Create chunks using DocumentChunker
        chunker = DocumentChunker(chunk_size=CHUNK_SIZE, overlap=OVERLAP)

        # Use page-aware chunking if page_markdown is available
        page_markdown = unchunked_result.get("content", {}).get("page_markdown")
        if page_markdown:
            logger.info("Using page-aware chunking to preserve text page numbers")
            chunked_result = chunker.chunk_docling_result_with_pages(
                unchunked_result, page_markdown
            )
        else:
            logger.info("Using standard chunking (no page info for text)")
            chunked_result = chunker.chunk_docling_result(unchunked_result)

        # Create unified nodes document with calculated SHA256 and user
        nodes_doc = create_nodes_document(
            chunked_result,
            file_name=file_name,
            file_sha256=file_sha256,
            user=user,
        )

        return nodes_doc

    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


# Generate output blob path for unified nodes file
def generate_output_blob_path(original_blob_path: str):
    # Extract path without extension
    path_without_ext = os.path.splitext(original_blob_path)[0]
    original_ext = os.path.splitext(original_blob_path)[1]

    # Create safe extension name
    safe_ext = original_ext.replace(".", "_")

    nodes_blob_path = f"{path_without_ext}{safe_ext}_nodes.json"

    return nodes_blob_path


# Process nodes with embedding and ingestion pipeline
def process_nodes_with_embeddings_and_ingestion(data):
    """
    Complete processing pipeline that adds metadata, embeddings, and ingests to Weaviate
    """
    logger.info("Starting embedding and ingestion pipeline...")

    # Step 1: Add metadata
    logger.info("Adding metadata to nodes...")
    nodes = add_metadata(data)

    # Step 2: Add embeddings
    logger.info("Adding embeddings to nodes...")
    nodes = add_embeddings(nodes)

    # Step 3: Ingest to Weaviate
    logger.info("Ingesting nodes to Weaviate...")
    ingest(nodes)

    logger.info("Embedding and ingestion pipeline completed successfully")
    return nodes


# Process a document from queue message
def preprocess(azqueue_message):
    # Initialize blob manager with connection string
    global blob_manager
    if blob_manager is None:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING environment variable not set"
            )
        blob_manager = BlobStorageManager(connection_string)

    # Parse the Event Grid message from the queue to extract blob URL
    blob_url = azqueue_message["data"]["url"]

    # Extract container and blob path from the blob URL
    container, blob_path = blob_manager.extract_blob_path_from_url(blob_url)
    logging.info(f"Processing blob: {container}/{blob_path}")

    # Get current blob metadata and tags, then mark as processing
    current_metadata, current_tags = blob_manager.get_blob_properties(
        container, blob_path
    )
    current_tags["ingest_state"] = "processing"
    blob_manager.update_blob_tags(container, blob_path, current_tags)

    try:
        # Download the blob content
        file_data = blob_manager.download_blob(container, blob_path)
        file_name = os.path.basename(blob_path)

        # Calculate SHA256 and extract username from blob path
        file_sha256 = calculate_sha256(file_data)
        user = extract_username_from_path(blob_path)

        logger.info(f"File SHA256: {file_sha256[:8]}...")
        logger.info(f"Extracted user: {user}")

        # Process the file using new unified nodes format
        nodes_doc = process_file_content(file_data, file_name, file_sha256, user)

        # Process nodes with embeddings and ingest to Weaviate
        try:
            process_nodes_with_embeddings_and_ingestion(nodes_doc)
            logger.info("Successfully processed nodes with embeddings and ingestion")
            embedding_status = "embedded"
        except Exception as e:
            logger.error(f"Failed to process nodes with embeddings: {e}")
            # Continue with file upload even if embedding/ingestion fails
            embedding_status = "embedding_failed"

        # Generate output blob path
        nodes_blob_path = generate_output_blob_path(blob_path)

        # Upload unified nodes JSON
        nodes_json_bytes = json.dumps(nodes_doc, indent=2).encode("utf-8")
        blob_manager.upload_processed_file(
            blob_name=nodes_blob_path,
            file_data=nodes_json_bytes,
            metadata=current_metadata,
            tags={
                "mime_type": "application/json",
                "processor": "local_processor",
                "content_type": "nodes",
                "ingest_state": embedding_status,
            },
            content_type="application/json",
        )

        # Update original blob tags to mark processing as complete
        current_tags["ingest_state"] = embedding_status
        current_tags["processor"] = "local_processor"
        blob_manager.update_blob_tags(container, blob_path, current_tags)

        logging.info(f"Successfully processed {blob_path.split('/')[-1]}")

    except Exception as e:
        # Mark original blob as failed
        current_tags["ingest_state"] = "failed"
        current_tags["error"] = str(e)
        blob_manager.update_blob_tags(container, blob_path, current_tags)

        logging.error(f"Failed to process {blob_path}: {e}")
        raise e


# Handle graceful shutdown signals
class WorkerShutdown:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    # Handle shutdown signals and set shutdown flag
    def _signal_handler(self, signum, _frame):
        logger.info(f"Received shutdown signal {signum}")
        self.shutdown = True


class DocumentWorker:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING environment variable not set"
            )

        self.queue_service = QueueServiceClient.from_connection_string(
            self.connection_string
        )
        self.queue_client = self.queue_service.get_queue_client(
            QUEUE_NAME, message_decode_policy=BinaryBase64DecodePolicy()
        )
        self.shutdown_handler = WorkerShutdown()

        # Ensure queue exists
        self._ensure_queue_exists()

        # Models are pre-cached in the Docker image during build
        self._initialize_cached_converters()

    # Create queue if it doesn't exist
    def _ensure_queue_exists(self):
        try:
            self.queue_client.create_queue()
            logger.info(f"Created queue: {QUEUE_NAME}")
        except Exception as e:
            if "QueueAlreadyExists" in str(e):
                logger.info(f"Queue {QUEUE_NAME} already exists")
            else:
                logger.error(f"Error creating queue: {e}")

    # Initialize converters using pre-cached models from Docker build
    def _initialize_cached_converters(self):
        global simple_converter, complex_converter

        logger.info("Initializing converters from cached models...")
        try:
            simple_converter = _create_simple_docling_converter()
            complex_converter = _create_docling_converter(complex_mode=True)
            logger.info("Converters initialized successfully from cached models")
        except Exception as e:
            logger.error(f"Failed to initialize converters: {e}")
            logger.error("Models may not be properly cached in the image")
            raise e

    # Parse queue message to extract Event Grid data
    def _parse_message(self, message):
        try:
            # Try to parse as JSON
            if hasattr(message, "content"):
                content = message.content
            else:
                content = message

            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Parse JSON content
            message_data = json.loads(content)

            # Handle different message formats
            if isinstance(message_data, list) and len(message_data) > 0:
                # Array of events
                event_data = message_data[0]
            elif isinstance(message_data, dict):
                # Single event
                event_data = message_data
            else:
                raise ValueError("Unexpected message format")

            # Validate required fields
            if "data" not in event_data or "url" not in event_data["data"]:
                raise ValueError("Message missing required 'data.url' field")

            return event_data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message as JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None

    # Process a single queue message
    def _process_message(self, message):
        try:
            # Parse the message
            event_data = self._parse_message(message)
            if not event_data:
                logger.error("Failed to parse message, skipping")
                return False

            # Log processing start
            blob_url = event_data["data"]["url"]
            logger.info(f"Processing blob: {blob_url}")

            # Process using preprocess function
            preprocess(event_data)

            logger.info(f"Successfully processed: {blob_url}")
            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    # One-shot worker that processes a single message and exits
    def run(self):
        logger.info("Starting one-shot document processing worker...")
        logger.info(f"Looking for a single message in queue '{QUEUE_NAME}'")

        max_wait_polls = 15  # Wait up to 30 seconds for a message (15 * 2 seconds)
        polls_without_message = 0

        while not self.shutdown_handler.shutdown:
            try:
                # Receive messages from queue
                logger.debug(f"Polling queue for messages...")
                messages = self.queue_client.receive_messages(
                    max_messages=MAX_MESSAGES,
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    timeout=10,
                )

                message_list = list(messages)

                if message_list:
                    # Process the first (and only) message
                    message = message_list[0]
                    logger.info(f"Received message: {message.id}")

                    # Process the message
                    success = self._process_message(message)

                    if success:
                        # Delete message from queue on success
                        try:
                            self.queue_client.delete_message(message)
                            logger.info(f"Deleted message: {message.id}")
                            logger.info(
                                "Successfully processed one message - worker exiting"
                            )
                            return  # Exit after processing one message
                        except Exception as e:
                            logger.error(f"Failed to delete message {message.id}: {e}")
                            logger.info(
                                "Message processing succeeded but deletion failed - worker exiting"
                            )
                            return
                    else:
                        # Let message become visible again for retry
                        logger.warning(f"Failed to process message: {message.id}")
                        logger.info("Message processing failed - worker exiting")
                        return

                else:
                    # No messages available
                    polls_without_message += 1
                    if polls_without_message >= max_wait_polls:
                        logger.info(
                            "No messages found after 30 seconds - worker exiting"
                        )
                        return

                    # Wait before next poll
                    time.sleep(POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                logger.info("Worker encountered error - exiting")
                return

        logger.info("Worker shutting down...")


# Entry point for the worker application
def main():
    try:
        worker = DocumentWorker()
        worker.run()
    except Exception as e:
        logger.error(f"Failed to start worker: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
