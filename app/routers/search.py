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
    print(f"Generative search result: {result}")
    if not result.success:
        raise HTTPException(status_code=500, detail=result.message)
    
    return result

@router.get("/collections")
async def list_collections():
    """List all available collections in Weaviate"""
    try:
        from db.weaviate_client import weaviate_client
        client = weaviate_client.get_client()
        
        # Get all collections
        collections = []
        for collection_name in client.collections.list_all():
            collections.append({
                "name": collection_name,
                "exists": client.collections.exists(collection_name)
            })
        
        return {
            "success": True,
            "collections": collections,
            "count": len(collections)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing collections: {str(e)}")