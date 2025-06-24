from pydantic import BaseModel
from typing import List, Optional, Any, Dict

class SearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 5
    collection_name: Optional[str] = "RAG_PROJECT2"

class SearchResponse(BaseModel):
    success: bool
    results: List[Dict[str, Any]]
    count: int
    message: Optional[str] = None

class GenerativeRequest(BaseModel):
    query: str
    limit: Optional[int] = 3
    task: Optional[str] = (
    "You are a biomedical question answering assistant. "
    "Based strictly on the provided context, answer the user's question with accurate and complete information. "
    "If the context contains the answer, extract it directly, including numbers, percentages, or other specific details. "
    "If the context does not contain the answer, respond exactly with: 'Information not available in the provided context.' "
    "Do not make assumptions, do not guess, and do not add any information not present in the context."
)

    collection_name: Optional[str] = "RAG_PROJECT2"

class GenerativeResponse(BaseModel):
    success: bool
    generated_text: Optional[str] = None
    source_results: List[Dict[str, Any]]
    count: int
    message: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    weaviate_ready: bool
    message: str
