import pytest
from unittest.mock import Mock, patch, MagicMock
from infra.model_loader import (
    get_weaviate_client,
    get_weaviate_rm,
    get_llm,
    get_text_embedding,
    run_replicate,
)


class TestWeaviateClient:
    @patch("infra.model_loader.connect_to_weaviate_cloud")
    @patch("infra.model_loader.wvc")
    def test_get_weaviate_client_with_params(self, mock_wvc, mock_connect):
        mock_client = Mock()
        mock_connect.return_value = mock_client
        mock_auth = Mock()
        mock_wvc.init.Auth.api_key.return_value = mock_auth

        result = get_weaviate_client("test_url", "test_key")

        mock_wvc.init.Auth.api_key.assert_called_once_with("test_key")
        mock_connect.assert_called_once_with(
            cluster_url="test_url", auth_credentials=mock_auth
        )
        assert result == mock_client

    @patch("infra.model_loader.connect_to_weaviate_cloud")
    @patch("infra.model_loader.wvc")
    def test_get_weaviate_client_with_env_vars(
        self, mock_wvc, mock_connect, mock_env_vars
    ):
        mock_client = Mock()
        mock_connect.return_value = mock_client
        mock_auth = Mock()
        mock_wvc.init.Auth.api_key.return_value = mock_auth

        result = get_weaviate_client()

        expected_url = mock_env_vars["WEAVIATE_URL"]
        expected_key = mock_env_vars["WEAVIATE_API_KEY"]

        mock_wvc.init.Auth.api_key.assert_called_once_with(expected_key)
        mock_connect.assert_called_once_with(
            cluster_url=expected_url, auth_credentials=mock_auth
        )
        assert result == mock_client


class TestWeaviateRM:
    @patch("infra.model_loader.get_weaviate_client")
    @patch("infra.model_loader.WeaviateRM")
    def test_get_weaviate_rm(self, mock_weaviate_rm, mock_get_client):
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_rm = Mock()
        mock_weaviate_rm.return_value = mock_rm

        result = get_weaviate_rm("test_collection", "test_tenant", "test_key")

        mock_get_client.assert_called_once_with(None, None)
        mock_weaviate_rm.assert_called_once_with(
            weaviate_client=mock_client,
            weaviate_collection_name="test_collection",
            tenant_id="test_tenant",
            weaviate_collection_text_key="test_key",
        )
        assert result == mock_rm


class TestLLM:
    @patch("infra.model_loader.LM")
    def test_get_llm_with_params(self, mock_lm):
        mock_model = Mock()
        mock_lm.return_value = mock_model

        result = get_llm("test_model", "test_key", "test_base", "test_version")

        mock_lm.assert_called_once_with(
            "test_model",
            api_key="test_key",
            api_base="test_base",
            api_version="test_version",
        )
        assert result == mock_model

    @patch("infra.model_loader.LM")
    def test_get_llm_with_env_vars(self, mock_lm, mock_env_vars):
        mock_model = Mock()
        mock_lm.return_value = mock_model

        result = get_llm("test_model")

        expected_key = mock_env_vars["OPENAI_API_KEY"]
        expected_base = mock_env_vars["OPENAI_API_BASE"]
        expected_version = mock_env_vars["OPENAI_API_VERSION"]

        mock_lm.assert_called_once_with(
            "test_model",
            api_key=expected_key,
            api_base=expected_base,
            api_version=expected_version,
        )
        assert result == mock_model


class TestTextEmbedding:
    @patch("infra.model_loader.embedding")
    def test_get_text_embedding_with_defaults(self, mock_embedding, mock_env_vars):
        mock_response = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}, {"embedding": [0.4, 0.5, 0.6]}]
        }
        mock_embedding.return_value = mock_response

        result = get_text_embedding(["test input"])

        expected_key = mock_env_vars["AZURE_EMBEDDING_KEY"]
        expected_endpoint = mock_env_vars["AZURE_EMBEDDING_ENDPOINT"]
        expected_version = mock_env_vars.get(
            "AZURE_EMBEDDING_API_VERSION", "2024-12-01-preview"
        )

        mock_embedding.assert_called_once_with(
            model="azure_ai/embed-v-4-0",
            input=["test input"],
            dimensions=1536,
            api_key=expected_key,
            api_base=expected_endpoint,
            api_version=expected_version,
        )
        assert result == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

    @patch("infra.model_loader.embedding")
    def test_get_text_embedding_with_params(self, mock_embedding):
        mock_response = {"data": [{"embedding": [0.7, 0.8, 0.9]}]}
        mock_embedding.return_value = mock_response

        result = get_text_embedding(
            "test input",
            dimensions=512,
            model_name="custom_model",
            api_key="custom_key",
            api_base="custom_base",
            api_version="custom_version",
        )

        mock_embedding.assert_called_once_with(
            model="custom_model",
            input="test input",
            dimensions=512,
            api_key="custom_key",
            api_base="custom_base",
            api_version="custom_version",
        )
        assert result == [[0.7, 0.8, 0.9]]


class TestReplicate:
    @patch("infra.model_loader.ReplicateClient")
    def test_run_replicate_with_api_key(self, mock_replicate_client):
        mock_client = Mock()
        mock_replicate_client.return_value = mock_client
        mock_client.run.return_value = {"result": "test_output"}

        result = run_replicate({"input": "test"}, "test_model", "test_api_key")

        mock_replicate_client.assert_called_once_with(api_key="test_api_key")
        mock_client.run.assert_called_once_with(
            "test_model", input={"input": "test"}, api_key="test_api_key"
        )
        assert result == {"result": "test_output"}

    @patch("infra.model_loader.ReplicateClient")
    def test_run_replicate_without_api_key(self, mock_replicate_client, mock_env_vars):
        mock_client = Mock()
        mock_replicate_client.return_value = mock_client
        mock_client.run.return_value = {"result": "test_output"}

        result = run_replicate({"input": "test"}, "test_model")

        expected_token = mock_env_vars.get("REPLICATE_API_TOKEN")

        mock_replicate_client.assert_called_once_with(api_key=expected_token)
        mock_client.run.assert_called_once_with(
            "test_model", input={"input": "test"}, api_key=expected_token
        )
        assert result == {"result": "test_output"}
