from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.weaviate_client import weaviate_client
from db.neo4j_client import neo4j_client
from routers import search, neo4j

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting FastAPI application...")
    try:
        weaviate_client.connect()
        print("Connected to Weaviate successfully")
        neo4j_client.connect()
        print("Connected to Neo4j successfully")
    except Exception as e:
        print(f"Failed to connect to databases: {e}")
    
    yield
    
    # Shutdown
    print("Shutting down FastAPI application...")
    weaviate_client.disconnect()
    print("Disconnected from Weaviate")
    neo4j_client.disconnect()
    print("Disconnected from Neo4j")

app = FastAPI(
    title="RAG API",
    description="FastAPI application with Weaviate and Neo4j integration for RAG operations",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Include routers
app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(neo4j.router, prefix="/api/neo4j", tags=["Neo4j"])

@app.get("/")
def read_root():
    return {
        "message": "RAG API is running!",
        "docs": "/docs",
        "health": {
            "weaviate": "/api/search/health",
            "neo4j": "/api/neo4j/health"
        }
    }

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
