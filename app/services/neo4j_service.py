from typing import List, Dict, Any
from db.neo4j_client import neo4j_client
from models.schema import Neo4jQueryRequest, Neo4jQueryResponse, Neo4jHealthResponse
from app.config import settings

class Neo4jService:
    
    @staticmethod
    def health_check() -> Dict[str, Any]:
        """Check Neo4j health status"""
        try:
            client = neo4j_client.get_driver()
            is_ready = neo4j_client.is_ready()
            return {
                "status": "healthy" if is_ready else "unhealthy",
                "neo4j_ready": is_ready,
                "message": "Neo4j is ready" if is_ready else "Neo4j is not ready"
            }
        except Exception as e:
            return {
                "status": "error",
                "neo4j_ready": False,
                "message": f"Error connecting to Neo4j: {str(e)}"
            }
    
    @staticmethod
    def execute_query(query_request: Neo4jQueryRequest) -> Neo4jQueryResponse:
        """Execute a Cypher query in Neo4j"""
        try:
            driver = neo4j_client.get_driver()
            
            with driver.session(database=settings.NEO4J_DATABASE) as session:
                result = session.run(
                    query_request.query,
                    parameters=query_request.parameters or {}
                )
                
                # Convert Neo4j records to dictionaries
                records = []
                for record in result:
                    record_dict = {}
                    for key, value in record.items():
                        # Handle Neo4j types conversion to Python native types
                        if hasattr(value, 'items'):  # If it's a Node or Relationship
                            record_dict[key] = dict(value.items())
                        else:
                            record_dict[key] = value
                    records.append(record_dict)
                
                return Neo4jQueryResponse(
                    success=True,
                    results=records,
                    count=len(records),
                    message=f"Successfully executed query with {len(records)} results"
                )
                
        except Exception as e:
            return Neo4jQueryResponse(
                success=False,
                results=[],
                count=0,
                message=f"Error executing query: {str(e)}"
            ) 