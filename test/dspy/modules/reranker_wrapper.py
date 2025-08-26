from json import dumps
from typing import List

from dspy import Module
from core.signatures import RerankChunks
from infra.model_loader import run_replicate


class BGEReranker(Module, signature=RerankChunks):
    def __init__(
        self,
        model_name: str = "yxzwayne/bge-reranker-v2-m3:7f7c6e9d18336e2cbf07d88e9362d881d2fe4d6a9854ec1260f115cabc106a8c",
        api_key: str | None = None,
    ):
        super().__init__(model_name=model_name, api_key=api_key)

    def forward(self, query: str, chunks: List[str], top_k: int = 5):
        scores = run_replicate(
            self.model_name,
            input={"input_list": dumps([[query, c] for c in chunks])},
            api_key=self.api_key,
        )
        return [p for _, p in sorted(zip(scores, chunks), reverse=True)][:top_k]
