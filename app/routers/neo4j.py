from fastapi import APIRouter, HTTPException
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.schema import (
    Neo4jQueryRequest,
    Neo4jQueryResponse,
    Neo4jHealthResponse
)
from services.neo4j_service import Neo4jService

router = APIRouter()

@router.get("/health", response_model=Neo4jHealthResponse)
async def health_check():
    """Check the health status of Neo4j connection"""
    health_data = Neo4jService.health_check()
    return Neo4jHealthResponse(**health_data)

@router.post("/query", response_model=Neo4jQueryResponse)
async def execute_query(query_request: Neo4jQueryRequest):
    """Execute a Cypher query in Neo4j"""
    if not query_request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    result = Neo4jService.execute_query(query_request)
    
    if not result.success:
        raise HTTPException(status_code=500, detail=result.message)
    
    return result 