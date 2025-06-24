import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
from app.config import settings

class WeaviateClient:
    def __init__(self):
        self.client = None
    
    def connect(self):
        """Connect to Weaviate Cloud"""
        try:
            self.client = weaviate.connect_to_weaviate_cloud(
                cluster_url=settings.WEAVIATE_URL,
                auth_credentials=Auth.api_key(settings.WEAVIATE_API_KEY),
                headers={"X-OpenAI-Api-Key": settings.OPENAI_API_KEY}
                # headers={"X-Cohere-Api-Key": settings.COHERE_API_KEY},

            )
            return self.client
        except Exception as e:
            print(f"Failed to connect to Weaviate: {e}")
            raise
    
    def disconnect(self):
        """Close Weaviate connection"""
        if self.client:
            self.client.close()
    
    def get_client(self):
        """Get the Weaviate client instance"""
        if not self.client:
            self.connect()
        return self.client
    
    def is_ready(self):
        """Check if Weaviate is ready"""
        try:
            return self.client.is_ready() if self.client else False
        except:
            return False

# Global instance
weaviate_client = WeaviateClient()