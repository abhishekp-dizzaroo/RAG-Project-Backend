import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    WEAVIATE_URL = os.environ.get("WEAVIATE_URL")
    WEAVIATE_API_KEY = os.environ.get("WEAVIATE_API_KEY")
    COHERE_API_KEY = os.environ.get("COHERE_APIKEY")
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "RAG_PROJECT_FLEXIBLE")
    LIMIT = os.environ.get("LIMIT", 2)
settings = Settings()
