import json
from typing import List, Dict, Any
from db.weaviate_client import weaviate_client
from models.schema import SearchRequest, SearchResponse, GenerativeRequest, GenerativeResponse

class WeaviateService:
    
    @staticmethod
    def health_check() -> Dict[str, Any]:
        """Check Weaviate health status"""
        try:
            client = weaviate_client.get_client()
            is_ready = client.is_ready()
            return {
                "status": "healthy" if is_ready else "unhealthy",
                "weaviate_ready": is_ready,
                "message": "Weaviate is ready" if is_ready else "Weaviate is not ready"
            }
        except Exception as e:
            return {
                "status": "error",
                "weaviate_ready": False,
                "message": f"Error connecting to Weaviate: {str(e)}"
            }
    
    @staticmethod
    def semantic_search(search_request: SearchRequest) -> SearchResponse:
        """Perform semantic search in Weaviate"""
        try:
            client = weaviate_client.get_client()
            collection = client.collections.get(search_request.collection_name)
            
            response = collection.query.near_text(
                query=search_request.query,
                limit=search_request.limit
            )
            
            results = []
            for obj in response.objects:
                result = {
                    "id": str(obj.uuid),
                    "properties": obj.properties,
                    "score": getattr(obj.metadata, 'distance', None)
                }
                results.append(result)
            
            return SearchResponse(
                success=True,
                results=results,
                count=len(results),
                message=f"Found {len(results)} results for query: '{search_request.query}'"
            )
            
        except Exception as e:
            return SearchResponse(
                success=False,
                results=[],
                count=0,
                message=f"Error performing search: {str(e)}"
            )
    
    @staticmethod
    def generative_search(gen_request: GenerativeRequest) -> GenerativeResponse:
        """Perform generative AI search in Weaviate"""
        try:
            client = weaviate_client.get_client()
            collection = client.collections.get(gen_request.collection_name)
            
            response = collection.generate.near_text(
                query=gen_request.query,
                limit=gen_request.limit,
                grouped_task=gen_request.task
            )
            
            # Extract source results
            source_results = []
            for obj in response.objects:
                result = {
                    "id": str(obj.uuid),
                    "properties": obj.properties
                }
                source_results.append(result)
            
            return GenerativeResponse(
                success=True,
                generated_text=response.generated,
                source_results=source_results,
                count=len(source_results),
                message=f"Generated response based on {len(source_results)} results"
            )
            
        except Exception as e:
            return GenerativeResponse(
                success=False,
                generated_text=None,
                source_results=[],
                count=0,
                message=f"Error performing generative search: {str(e)}"
            )
