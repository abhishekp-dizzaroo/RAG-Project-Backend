from neo4j import GraphDatabase
from app.config import settings

class Neo4jClient:
    def __init__(self):
        self.driver = None
    
    def connect(self):
        """Connect to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )
            return self.driver
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            raise
    
    def disconnect(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
    
    def get_driver(self):
        """Get the Neo4j driver instance"""
        if not self.driver:
            self.connect()
        return self.driver
    
    def is_ready(self):
        """Check if Neo4j is ready"""
        try:
            if not self.driver:
                self.connect()
            with self.driver.session() as session:
                result = session.run("RETURN 1")
                return bool(result.single())
        except:
            return False

# Global instance
neo4j_client = Neo4jClient() 