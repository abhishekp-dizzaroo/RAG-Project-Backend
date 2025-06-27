from fastapi import APIRouter, HTTPException, Depends
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.schema import (
    SearchRequest,SearchResponse,
    GenerativeRequest,
    GenerativeResponse, HealthResponse
)
from services.weaviate_service import WeaviateService

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Check the health status of Weaviate connection"""
    health_data = WeaviateService.health_check()
    return HealthResponse(**health_data)

@router.post("/search", response_model=SearchResponse)
async def semantic_search(search_request: SearchRequest):
    """Perform semantic search using Weaviate"""
    if not search_request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    result = WeaviateService.semantic_search(search_request)
    
    if not result.success:
        raise HTTPException(status_code=500, detail=result.message)
    
    return result

@router.post("/generate", response_model=GenerativeResponse)
async def generative_search(gen_request: GenerativeRequest):
    """Perform generative AI search using Weaviate and Cohere"""
    if not gen_request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    result = WeaviateService.generative_search(gen_request)
    print("*"*200)
    print(f"Generated response: {result}")
    if not result.success:
        raise HTTPException(status_code=500, detail=result.message)
    
    return result

