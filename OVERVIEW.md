# Project Astryx — Overview

**Multimodal RAG system on Azure.** Ingest documents of any modality → embed → store in Weaviate → query via DSPy-orchestrated pipeline with reranking → return cited, conversational answers with full MLflow tracing.

---

## What This Is

Two-pipeline architecture:

1. **Ingestion** — Upload files → process → embed → store in Weaviate vector DB
2. **Inference** — User query → DSPy RAG pipeline → rerank → LLM response

Target modalities: Text, Images (done), Audio, Video (planned, not started).

---

## Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Document processing | Docling (PDFs), MarkItDown (rest) | Docling adaptive 2-pass |
| Chunking | Custom semantic (header-aware) | With intelligent image association |
| Embedding | Azure AI `embed-v-4-0` (text + image) via LiteLLM | 1536 dims |
| Vector DB | Weaviate Cloud (`vectordbjunes`, GCP us-east1) | Multi-tenant, HNSW dynamic index |
| Graph DB | Neo4j community (Azure VM + Docker) | Planned, not integrated |
| Inference framework | DSPy | Weaviate retrieval bypasses WeaviateRM (no BYOV+MT support) |
| Reranker | BGE-reranker-v2-m3 via Replicate | Colbert dropped (redundant at scale we have) |
| LLM | Azure OpenAI GPT-4o mini via LiteLLM | All models unified through LiteLLM |
| Chat history | CosmosDB | Planned, not implemented |
| Observability | MLflow (Azure ML Studio) | Planned |
| IaC | Terraform | Planned |
| Infra | Azure Container Apps (KEDA-scaled workers), Azure Blob, Azure Storage Queue, Event Grid | |

---

## Architecture

### Ingestion (BUILT)

```
User upload → FastAPI /upload
  → SHA256 dedupe → Azure Blob (raw container)
  → Event Grid → Storage Queue
  → KEDA-scaled ACA worker
  → Adaptive Docling (easy mode → triage → complex mode for image/table pages)
  → Semantic chunker (header-aware, sentence-boundary, image↔text association by OCR overlap)
  → Batch embed: text (96/batch) + images (20/batch) via Azure embed-v-4-0
  → Threaded image upload to processed blob (12 workers)
  → Weaviate multi-tenant ingest (TextChunk + ImageChunk with bidirectional refs)
  → Blob tags track state: processing → embedded | embedding_failed
```

### Inference (PARTIAL — DSPy scripts only, no served API)

```
User query
  → DSPy QueryRewriter / QuestionDecomposer
  → Weaviate hybrid retrieval (manual, bypasses WeaviateRM for MT+BYOV)
  → BGE-reranker via Replicate
  → LLM (GPT-4o mini via LiteLLM)
  → CosmosDB chat history read/write  ← not implemented
  → Response with citations
```

---

## Key Decisions Made

**DB selection (4/27/25):** Research confirmed Weaviate as vector DB (best multimodal + AI search), Neo4j for graph. Neither Weaviate alone nor a combined DB covered both; chose Weaviate + Neo4j separately.

**Embedding models:**
- Text: Azure `embed-v-4-0` (closed source, Azure hosted). e5-mistral-7b considered but not in Azure catalog, would need self-hosting.
- Image: `embed-v-4-0` (same model, multimodal). OpenCLIP ViT-g-14 as backup.
- Audio: CLAP htsat-fused (planned). Video: InternVideo2-1B (planned). Neither implemented.

**Preprocessing:** Docling for all PDFs (superior table/image extraction). MarkItDown for everything else. Adaptive 2-pass was the source of the 16840% speedup (86s → 5s) — only runs expensive pipeline on complex pages.

**Reranking:** Initial plan was 2-stage (ColBERT → BGE). Revised: ColBERT dropped (redundant when not scanning 1000s of chunks). BGE via Replicate confirmed. HuggingFace Inference API was tried first — BAAI/bge-reranker-v2-m3 not on serverless API, FlagEmbedding SDK worked but 6s inference vs 0.0015s Replicate → Replicate chosen.

**DSPy integration:** Native WeaviateRM doesn't support multi-tenancy + bring-your-own-vectors. Workaround: custom query scripts pull chunks from Weaviate, pass manually into DSPy context.

**Model routing:** Everything through LiteLLM. No SDK diversity. Azure models preferred; Replicate exception for reranker (no Azure equivalent for BAAI/bge-reranker-v2-m3 exact).

**Schema:** TextChunk and ImageChunk as separate Weaviate collections per modality. Bidirectional cross-references (`hasImages ↔ belongsToText`). Deterministic uuid5 IDs from node_id. Multi-tenant from day 1.

**Adapters:** ChatAdapter, JSONAdapter, TwoStepAdapter — no custom adapters. BootstrapFinetune treated as separate script set.

---

## What Was Built

### Ingestion (complete)
- `src/upload_api/` — FastAPI upload endpoint, SHA256 dedupe, blob state tagging
- `src/ingestion/doc_processing/processor.py` — adaptive 2-pass Docling, page triaging, image extraction
- `src/ingestion/doc_processing/chunker.py` — semantic header-aware chunker, image↔text OCR association
- `src/ingestion/llamaindex_embed/embed_nodes.py` — batch multimodal embedding
- `src/ingestion/weaviate_ingest/` — schema, collection/tenant management, object builder, batch uploader, bidirectional ref wiring
- `src/ingestion/utils/migrate.py` — full backup/restore with vectors + cross-refs + cross-cluster tenant mapping
- `src/ingestion/utils/query.py` — keyword, vector, hybrid, near-object query scripts

### DSPy layer (partial)
- `test/dspy/core/signatures.py` — 13 signatures: QuestionClassifier, QueryRewriter, QuestionDecomposer, RetrievalPlanner, ChunkReranker, ChunkSummarizer, BasicAnswerer, RAGAnswerer, ConversationalAnswerer, CitedAnswerer, SelfCritic, ToolSelector, ArgumentBuilder
- `test/dspy/infra/model_loader.py` — Weaviate client, LLM loader (LiteLLM), text embedding, Replicate reranker
- `test/dspy/modules/reranker_wrapper.py` — BGE reranker DSPy wrapper (broken import: imports `RerankChunks` but signature is named `ChunkReranker`)
- `test/dspy/tests/` — comprehensive mocked unit tests for infra + reranker

### Weaviate live
- Instance: `vectordbjunes` on Weaviate Cloud (GCP us-east1), v1.31.1
- Features: async indexing, async replication

---

## What's Left

### Inference / DSPy (nothing wired end-to-end)
- `src/inference/` — **empty directory**, no serving layer
- `test/dspy/pipelines/` — empty (no RAG pipeline composed)
- `test/dspy/optimize/` — empty
- `test/dspy/evaluation/` — empty
- `test/dspy/utils/config.py` — empty
- Fix broken import in `reranker_wrapper.py` (`RerankChunks` → `ChunkReranker`)
- CosmosDB schema design + implementation (chat history, feedback, ground truth datasets)
- MLflow integration (Azure ML Studio compute not yet provisioned for DSPy)
- End-to-end DSPy pipeline: Classify → Rewrite → Retrieve → Rerank → Answer → log
- Evaluation: ground truth Q/A pair generation, RAGAS metrics, tool call precision/recall
- Optimization: MIPROv2, BetterTogether (after eval baseline exists)

### Infrastructure
- Neo4j not connected to anything (deployed but unused)
- Terraform IaC not started
- Azure ML Studio compute for DSPy not provisioned (T4 quota request in progress as of last message)
- No retry / dead-letter queue for `embedding_failed` blob state
- No integration tests against live Weaviate

### Modalities
- Audio: CLAP model selected, not implemented
- Video: InternVideo2 selected, not implemented

---

## What Failed / Was Pivoted

| Decision | What happened |
|---|---|
| LlamaIndex as embedding layer | Replaced by native LiteLLM — simpler, faster |
| ColBERT as first-stage reranker | Dropped — added latency, no value at current chunk scale |
| HuggingFace Serverless for BGE reranker | BAAI/bge-reranker-v2-m3 not available; FlagEmbedding SDK was 6s vs Replicate 0.0015s → Replicate |
| One-collection-per-dataset | Dropped for multi-tenancy — better isolation, security, RBAC |
| Azure ML Studio compute for DSPy dev | Quota issues (T4 not allocated); devwork done locally as workaround |
| LlamaIndex WeaviateRM | Can't use with multi-tenancy + BYOV — replaced by custom query scripts |
| Weaviate native vectorizer | Decided to pass vectors manually (LiteLLM → Weaviate), keep vectorizer as fallback |
| image_data_base64 in Weaviate schema | Replaced by `image_url` pointing to processed blob — avoids bloating vector objects |
| replicate reranker → HF | HF doesn't host model → back to Replicate |

---

## Code Quality Notes

- Two `BlobStorageManager` classes exist (`upload_api/` vs `ingestion/utils/`) — not consolidated
- `chunk_docling_result()` and `chunk_docling_result_with_pages()` duplicate image node construction
- Silent `except Exception: pass` on Weaviate reference add (`worker.py:164`)
- `prewarm_models.py` exists but is never called
- Quantization in Weaviate commented out (`collection.py`)
- Archive (`test/archive/`) has superseded implementations — fine to delete

---

## DSPy Script Roadmap (planned, 7/8/25)

| Batch | Goal | Status |
|---|---|---|
| 1 | Core DNA: config, model_loader, signatures, adapters, chat_history | Partial (signatures + model_loader done) |
| 2 | Reasoning modules: predict, CoT, reranker, query_generator, async_utils | Not started |
| 3 | Metrics vertical slice + MLflow smoke test | Not started |
| 4 | Hello-RAG pipeline end-to-end | Not started |
| 5 | Multi-hop, agent tools, program-of-thought, ensemble, tool-call eval | Not started |
| 6 | Optimization: few-shot, MIPROv2, finetune | Not started |
| 7 | Tests + CI + Docker | Not started |
