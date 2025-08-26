# DSPy Function Tests

This directory contains comprehensive pytest tests for all functions in the DSPy project.

## Test Coverage

### Infrastructure Tests (`test_infra_model_loader.py`)

-   **get_weaviate_client()** - Tests Weaviate client creation with/without parameters
-   **get_weaviate_rm()** - Tests Weaviate retrieval module setup
-   **get_llm()** - Tests language model initialization
-   **get_text_embedding()** - Tests text embedding generation
-   **run_replicate()** - Tests Replicate API calls

### Module Tests (`test_modules_reranker.py`)

-   **rerank()** - Tests passage reranking with various scenarios
    -   Basic functionality with scoring
    -   Custom models and API keys
    -   Edge cases (empty passages, k > passage count)
    -   Input format validation

### Core Tests (`test_core_signatures.py`)

-   **GenerateContextualAnswer** - Tests basic signature definition
-   **GenerateContextualAnswerWithChatHistory** - Tests extended signature
-   Field validation and DSPy integration

### Utils Tests (`test_utils_config.py`)

-   **get_client()** - Tests configuration and client setup
-   Environment variable handling
-   Error scenarios and edge cases

## Prerequisites

Install test dependencies:

```bash
pip install -r tests/requirements.txt
```

## Running Tests

### Basic Usage

```bash
# Run all tests
python run_tests.py

# Run with verbose output
python run_tests.py --verbose

# Run specific test file
python -m pytest tests/test_infra_model_loader.py -v
```

### Test Categories

```bash
# Run only unit tests (fast, no external dependencies)
python run_tests.py --unit

# Run only integration tests (may require external services)
python run_tests.py --integration
```

### Coverage Reports

```bash
# Generate coverage report
python run_tests.py --coverage
```

### Other Options

```bash
# Stop on first failure
python run_tests.py --fail-fast

# Run tests in parallel (requires pytest-xdist)
python run_tests.py --parallel
```

## Test Design

All tests use **mocking** to avoid external dependencies:

-   No actual API calls to Weaviate, OpenAI, or Replicate
-   No real environment variables required
-   Fast execution with predictable results
-   Tests focus on function logic and parameter handling

## Configuration

-   **pytest.ini** - Main pytest configuration
-   **conftest.py** - Shared fixtures and test setup
-   **requirements.txt** - Test-specific dependencies

## Adding New Tests

1. Create test file: `test_module_name.py`
2. Import the module to test
3. Use mocking for external dependencies
4. Follow the existing pattern of test classes and methods
5. Add appropriate markers (@pytest.mark.unit or @pytest.mark.integration)

## Continuous Integration

These tests are designed to run in CI/CD environments without requiring:

-   External API credentials
-   Network connectivity
-   Specific environment setup

All external dependencies are mocked for reliable, fast testing.
