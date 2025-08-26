from dspy import InputField, OutputField, Signature
from typing import List, Dict, Any
from enum import Enum


class Route(str, Enum):
    RAG = "rag"  # needs Weaviate retrieval + rerank
    MATH = "math"  # pure calculator / code-exec
    TOOL_CALL = "tool"  # ReAct with external API (weather, stock, etc.)
    NO_CONTEXT = "no_context"  # trivial Q; LLM can answer from priors


# QUERY PROCESSING


class QuestionClassifier(Signature):
    """
    Decide *early* what processing path the user question should take.
    The routing enum is tuned to our stack:
        • "rag"  → run QueryGenerator → Weaviate → Rerank
        • "math" → skip RAG, go straight to ProgramOfThought / calculator
        • "tool" → ReAct loop to choose and call a domain API
        • "no_context" → direct Predict with temp=0
    """

    question: str = InputField(desc="Raw user question")
    route: Route = OutputField(desc="Chosen processing path")


class QueryRewriter(Signature):
    """Rewrite or expand the user's question so the vector / keyword
    (hybrid) search layer recalls more relevant chunks."""

    question = InputField(desc="Original user question")
    search_query = OutputField(desc="Improved query for retrieval")


class QuestionDecomposer(Signature):
    """Break a complex, multi-hop question into sequential sub-queries."""

    question = InputField(desc="Possibly multi-hop user question")
    followup_queries = OutputField(desc="Ordered list of simpler questions")


# RETRIEVAL & RERANKING


class RetrievalPlanner(Signature):
    """
    Plan 'how hard' the hybrid search should work for the given question.

    Outputs map 1-to-1 to Weaviate's `queryHybrid()` args:

        • limit   - total # results (vector + lexical blended)
        • alpha   - 0 ⇒ rely on BM25, 1 ⇒ rely on ANN vectors

    Example guidance:
        trivia            → limit=10 , alpha=0.2
        long-tail science → limit=50 , alpha=0.8
        math / tool path  → limit=0  , alpha=0   (skip search)
    """

    question: str = InputField(desc="Possibly rewritten user question")
    route: Route = InputField(desc="Classifier output ('rag', 'math', …)")

    limit: int = OutputField(desc="Total results to retrieve")
    alpha: float = OutputField(desc="Blend 0=BM25, 1=Vector")


class ChunkReranker(Signature):
    """Return the K passages ranked most relevant to the query."""

    query: str = InputField(desc="Original or rewritten search query")
    chunks: List[str] = InputField(desc="List of raw chunks")
    top_chunks: List[str] = OutputField(
        desc="Chunks ranked by relevance, highest first"
    )


# CONTENT PROCESSING


class ChunkSummarizer(Signature):
    """Condense K chunks into a short reference summary."""

    chunks = InputField(desc="List of context chunks")
    summary = OutputField(desc="Concise summary")


# ANSWER GENERATION


class BasicAnswerer(Signature):
    """Generate an answer to a question"""

    question = InputField(desc="User question")
    answer = OutputField(desc="Answer to the question")


class RAGAnswerer(Signature):
    """Generate an answer to a question based on a set of context chunks"""

    context = InputField(desc="ANN-based context chunks")
    question = InputField(desc="User question")
    answer = OutputField(desc="Answer to the question")


class ConversationalAnswerer(Signature):
    """Generate an answer to a question based on a set of context chunks and conversation history"""

    chat_history = InputField(desc="Chat history")
    context = InputField(desc="ANN-based context chunks")
    question = InputField(desc="User question")
    answer = OutputField(desc="Answer to the question")


class CitedAnswerer(Signature):
    """Return both the answer and the IDs of chunks that support it."""

    question = InputField()
    answer = OutputField()
    sources: List[str] = OutputField(desc="doc IDs or chunk hashes")


class SelfCritic(Signature):
    """
    Classic 'self-refine' pass: look at the draft answer plus the
    supporting context and produce a cleaner, more precise revision.
    You can optionally have the critic add missing citations
    ([1], [2]) or tighten wording.
    """

    draft_answer: str = InputField(desc="First-pass answer from BasicAnswerer")
    question: str = InputField(desc="Original user question")
    context: List[str] = InputField(desc="Top-N retrieved or reranked chunks")

    revised_answer: str = OutputField(desc="Polished, citation-correct answer")


# AGENT & TOOLS


class ToolSelector(Signature):
    """Given the user request, decide which named tool to invoke next."""

    tools = InputField(desc="Available tool names and brief docs")
    user_request = InputField(desc="Current user message")
    chosen_tool = OutputField(desc="Name of the tool to run (or 'none')")


class ArgumentBuilder(Signature):
    """Generate the argument dict for the chosen tool."""

    chosen_tool = InputField(desc="Tool name returned by ToolSelector")
    user_request = InputField(desc="Current user message")
    arguments = OutputField(desc="JSON-serialisable args for that tool")
