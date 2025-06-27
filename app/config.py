import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    WEAVIATE_URL = os.environ.get("WEAVIATE_URL")
    WEAVIATE_API_KEY = os.environ.get("WEAVIATE_API_KEY")
    COHERE_API_KEY = os.environ.get("COHERE_APIKEY")
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME")
    LIMIT = 3
    
    # Neo4j settings
    NEO4J_URI = os.environ.get("NEO4J_URI")
    NEO4J_USER = os.environ.get("NEO4J_USER")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
    NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

settings = Settings()
