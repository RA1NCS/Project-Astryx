import pytest
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add the dspy directory to Python path so imports work correctly
dspy_root = Path(__file__).parent.parent
sys.path.insert(0, str(dspy_root))

# Load the actual .env file from project root
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup the test environment before running any tests"""
    # Set up any global test configuration here
    os.environ["TESTING"] = "true"
    yield
    # Cleanup after all tests are done
    if "TESTING" in os.environ:
        del os.environ["TESTING"]


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to provide real environment variables from .env file for tests"""
    # Load real values from .env file
    env_vars = {
        "WEAVIATE_URL": os.getenv("WEAVIATE_URL"),
        "WEAVIATE_API_KEY": os.getenv("WEAVIATE_API_KEY"),
        "WEAVIATE_URL_2": os.getenv("WEAVIATE_URL_2"),
        "WEAVIATE_API_KEY_2": os.getenv("WEAVIATE_API_KEY_2"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "OPENAI_API_BASE": os.getenv("OPENAI_API_BASE"),
        "OPENAI_API_VERSION": os.getenv("OPENAI_API_VERSION"),
        "AZURE_EMBEDDING_KEY": os.getenv("AZURE_EMBEDDING_KEY"),
        "AZURE_EMBEDDING_ENDPOINT": os.getenv("AZURE_EMBEDDING_ENDPOINT"),
        "AZURE_EMBEDDING_API_VERSION": "2024-12-01-preview",  # Default version
        "REPLICATE_API_TOKEN": os.getenv("REPLICATE_API_TOKEN"),
        "REPLICATE_API_KEY": os.getenv(
            "REPLICATE_API_TOKEN"
        ),  # Some tests expect this name
        "HF_URL": os.getenv("HF_URL"),
        "HF_TOKEN": os.getenv("HF_TOKEN"),
        "COHERE_APIKEY": os.getenv("COHERE_APIKEY"),
        "AZURE_STORAGE_CONNECTION_STRING": os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    }

    # Set environment variables for the test, filtering out None values
    for key, value in env_vars.items():
        if value is not None:
            monkeypatch.setenv(key, value)

    return {k: v for k, v in env_vars.items() if v is not None}


@pytest.fixture
def sample_passages():
    """Fixture providing sample text passages for testing"""
    return [
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning is a subset of artificial intelligence.",
        "Python is a popular programming language for data science.",
        "Natural language processing enables computers to understand human language.",
        "Deep learning uses neural networks with multiple layers.",
    ]


@pytest.fixture
def sample_query():
    """Fixture providing a sample query for testing"""
    return "What is machine learning?"
