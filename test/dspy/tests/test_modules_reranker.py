import pytest
from unittest.mock import Mock, patch
from modules.reranker_wrapper import rerank


class TestReranker:
    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_basic_functionality(self, mock_run_replicate):
        # Mock the replicate response
        mock_run_replicate.return_value = [2.5, 0.8, 4.1, 1.2]

        passages = [
            "The sky is blue",
            "Machine learning is powerful",
            "This is the most relevant passage",
            "Random text here",
        ]
        query = "What is most relevant?"

        result = rerank(passages, query, k=3)

        # Should return top 3 passages sorted by score (highest first)
        expected = [
            "This is the most relevant passage",  # score 4.1
            "The sky is blue",  # score 2.5
            "Random text here",  # score 1.2
        ]

        assert result == expected
        assert len(result) == 3

        # Verify run_replicate was called correctly
        mock_run_replicate.assert_called_once()
        call_args = mock_run_replicate.call_args
        assert (
            call_args[0][0]
            == "yxzwayne/bge-reranker-v2-m3:7f7c6e9d18336e2cbf07d88e9362d881d2fe4d6a9854ec1260f115cabc106a8c"
        )

        # Check input structure
        input_data = call_args[1]["input"]
        assert "input_list" in input_data

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_with_custom_model(self, mock_run_replicate):
        mock_run_replicate.return_value = [1.0, 3.0]

        passages = ["passage1", "passage2"]
        query = "test query"
        custom_model = "custom/model:version"

        result = rerank(passages, query, model=custom_model, k=2)

        # Verify custom model was used
        call_args = mock_run_replicate.call_args
        assert call_args[0][0] == custom_model

        # Check result ordering (passage2 should be first with score 3.0)
        assert result == ["passage2", "passage1"]

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_with_custom_api_key(self, mock_run_replicate):
        mock_run_replicate.return_value = [2.0]

        passages = ["test passage"]
        query = "test"
        custom_api_key = "custom_key_123"

        result = rerank(passages, query, api_key=custom_api_key)

        # Verify custom API key was passed
        call_args = mock_run_replicate.call_args
        assert call_args[1]["api_key"] == custom_api_key

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_with_env_api_key(self, mock_run_replicate, mock_env_vars):
        mock_run_replicate.return_value = [1.5]

        passages = ["test passage"]
        query = "test"

        result = rerank(passages, query)

        # Verify environment variable was used
        expected_api_key = mock_env_vars.get("REPLICATE_API_TOKEN")
        call_args = mock_run_replicate.call_args
        assert call_args[1]["api_key"] == expected_api_key

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_k_larger_than_passages(self, mock_run_replicate):
        mock_run_replicate.return_value = [1.0, 2.0]

        passages = ["passage1", "passage2"]
        query = "test"

        # Request more results than available passages
        result = rerank(passages, query, k=10)

        # Should return all available passages
        assert len(result) == 2
        assert result == ["passage2", "passage1"]  # sorted by score

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_empty_passages(self, mock_run_replicate):
        mock_run_replicate.return_value = []

        passages = []
        query = "test"

        result = rerank(passages, query)

        assert result == []

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_single_passage(self, mock_run_replicate):
        mock_run_replicate.return_value = [3.5]

        passages = ["single passage"]
        query = "test"

        result = rerank(passages, query, k=1)

        assert result == ["single passage"]
        assert len(result) == 1

    @patch("modules.reranker_wrapper.run_replicate")
    def test_rerank_input_format(self, mock_run_replicate):
        mock_run_replicate.return_value = [1.0, 2.0, 3.0]

        passages = ["A", "B", "C"]
        query = "Q"

        rerank(passages, query)

        # Verify the input format is correct
        call_args = mock_run_replicate.call_args
        input_data = call_args[1]["input"]

        # Parse the JSON string to verify structure
        import json

        input_list = json.loads(input_data["input_list"])

        expected_format = [["Q", "A"], ["Q", "B"], ["Q", "C"]]
        assert input_list == expected_format
