from pydantic import BaseModel
from typing import List, Optional, Any, Dict

class SearchRequest(BaseModel):
    query: str

class SearchResponse(BaseModel):
    success: bool
    results: List[Dict[str, Any]]
    count: int
    message: Optional[str] = None

class GenerativeRequest(BaseModel):
    query: str

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
