from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.weaviate_client import weaviate_client
from routers import search

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting FastAPI application...")
    try:
        weaviate_client.connect()
        print("Connected to Weaviate successfully")
    except Exception as e:
        print(f"Failed to connect to Weaviate: {e}")
    
    yield
    
    # Shutdown
    print("Shutting down FastAPI application...")
    weaviate_client.disconnect()
    print("Disconnected from Weaviate")

app = FastAPI(
    title="Weaviate RAG API",
    description="FastAPI application with Weaviate integration for RAG operations",
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

@app.get("/")
def read_root():
    return {
        "message": "Weaviate RAG API is running!",
        "docs": "/docs",
        "health": "/api/search/health"
    }

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
