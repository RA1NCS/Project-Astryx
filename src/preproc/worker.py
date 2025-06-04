import logging
import json
import os
import tempfile
import time
import signal
import sys
from dotenv import load_dotenv
from azure.storage.queue import QueueServiceClient, BinaryBase64DecodePolicy

from utils.blob_storage import BlobStorageManager
from processors.processor import (
    process_document,
    _create_docling_converter,
    _create_simple_docling_converter,
)
from processors.chunker import (
    DocumentChunker,
    extract_text_chunks,
    extract_image_chunks,
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
MAX_RETRIES = 3
VISIBILITY_TIMEOUT = 180
MAX_MESSAGES = 1

# Initialize managers lazily to avoid startup errors
blob_manager = None
simple_converter = None
complex_converter = None


def process_file_content(file_data: bytes, file_name: str):
    """Process file content using the local processor functionality."""
    # Create temporary file
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

        # Create chunks using DocumentChunker
        chunker = DocumentChunker(chunk_size=CHUNK_SIZE, overlap=OVERLAP)
        chunked_result = chunker.chunk_docling_result(unchunked_result)

        # Extract text and image JSONs directly
        text_json = extract_text_chunks(chunked_result)
        image_json = extract_image_chunks(chunked_result)

        return text_json, image_json

    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def generate_output_blob_paths(original_blob_path: str):
    """Generate output blob paths for text and image files."""
    # Extract path without extension
    path_without_ext = os.path.splitext(original_blob_path)[0]
    original_ext = os.path.splitext(original_blob_path)[1]

    # Create safe extension name
    safe_ext = original_ext.replace(".", "_")

    text_blob_path = f"{path_without_ext}{safe_ext}_text.json"
    image_blob_path = f"{path_without_ext}{safe_ext}_images.json"

    return text_blob_path, image_blob_path


def preprocess(azqueue_message):
    """Process a document from queue message."""
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

        # Process the file using local processor functionality
        text_json, image_json = process_file_content(file_data, file_name)

        # Generate output blob paths
        text_blob_path, image_blob_path = generate_output_blob_paths(blob_path)

        # Upload text JSON
        text_json_bytes = json.dumps(text_json, indent=2).encode("utf-8")
        blob_manager.upload_processed_file(
            blob_name=text_blob_path,
            file_data=text_json_bytes,
            metadata=current_metadata,
            tags={
                "mime_type": "application/json",
                "processor": "local_processor",
                "content_type": "text",
                "ingest_state": "done",
            },
            content_type="application/json",
        )

        # Upload image JSON
        image_json_bytes = json.dumps(image_json, indent=2).encode("utf-8")
        blob_manager.upload_processed_file(
            blob_name=image_blob_path,
            file_data=image_json_bytes,
            metadata=current_metadata,
            tags={
                "mime_type": "application/json",
                "processor": "local_processor",
                "content_type": "images",
                "ingest_state": "done",
            },
            content_type="application/json",
        )

        # Update original blob tags to mark processing as complete
        current_tags["ingest_state"] = "done"
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


class WorkerShutdown:
    """Handle graceful shutdown signals"""

    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
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

    def _reconnect_queue_client(self):
        """Reconnect queue client if needed"""
        try:
            self.queue_client = self.queue_service.get_queue_client(
                QUEUE_NAME, message_decode_policy=BinaryBase64DecodePolicy()
            )
            logger.info("Queue client reconnected")
        except Exception as e:
            logger.error(f"Failed to reconnect queue client: {e}")

    def _ensure_queue_exists(self):
        """Create queue if it doesn't exist"""
        try:
            self.queue_client.create_queue()
            logger.info(f"Created queue: {QUEUE_NAME}")
        except Exception as e:
            if "QueueAlreadyExists" in str(e):
                logger.info(f"Queue {QUEUE_NAME} already exists")
            else:
                logger.error(f"Error creating queue: {e}")

    def _initialize_cached_converters(self):
        """Initialize converters using pre-cached models from Docker build."""
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

    def _parse_message(self, message):
        """Parse queue message to extract Event Grid data"""
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

    def _process_message(self, message):
        """Process a single queue message"""
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

    def run(self):
        """One-shot worker - processes a single message and exits"""
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


def main():
    """Entry point"""
    try:
        worker = DocumentWorker()
        worker.run()
    except Exception as e:
        logger.error(f"Failed to start worker: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
