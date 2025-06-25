from pydantic import BaseModel
from typing import List, Optional, Any, Dict

class SearchRequest(BaseModel):
    query: str

class SearchResponse(BaseModel):
    success: bool
    results: List[Dict[str, Any]]
    count: int
    message: str

class GenerativeRequest(BaseModel):
    query: str

class GenerativeResponse(BaseModel):
    success: bool
    generated_text: Optional[str]
    source_results: List[Dict[str, Any]]
    count: int
    message: str

class HealthResponse(BaseModel):
    status: str
    weaviate_ready: bool
    message: str

# Neo4j specific models
class Neo4jQueryRequest(BaseModel):
    query: str
    parameters: Optional[Dict[str, Any]] = None

class Neo4jQueryResponse(BaseModel):
    success: bool
    results: List[Dict[str, Any]]
    count: int
    message: str

class Neo4jHealthResponse(BaseModel):
    status: str
    neo4j_ready: bool
    message: str
