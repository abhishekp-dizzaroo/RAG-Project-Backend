import json
import os
import sys
from datetime import datetime
from tqdm import tqdm
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.db.neo4j_client import neo4j_client

# Configuration
JSON_DIR = "json_files"
CHUNK_SIZE = 2000  # Maximum size for text chunks

def flatten_dict(d, parent_key='', sep='_'):
    """Recursively flatten nested dictionaries."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def chunk_text(text, max_size=CHUNK_SIZE):
    """Split text into chunks if it exceeds max_size."""
    if not isinstance(text, str) or len(text) <= max_size:
        return [text]
    
    words = text.split()
    chunks = []
    current_chunk = []
    current_size = 0
    
    for word in words:
        word_size = len(word) + 1  # +1 for space
        if current_size + word_size > max_size and current_chunk:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_size = word_size
        else:
            current_chunk.append(word)
            current_size += word_size
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def clean_property_value(value):
    """Clean and format property values for Neo4j."""
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if isinstance(value, (int, float, bool)):
        return value
    # Try to parse dates
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return str(value)
    return str(value)

def create_constraints(session):
    """Create necessary constraints in Neo4j."""
    constraints = [
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE"
    ]
    
    for constraint in constraints:
        try:
            session.run(constraint)
        except Exception as e:
            print(f"Warning: Constraint creation failed: {e}")

def process_json_file(filepath, filename, session):
    """Process a single JSON file and create nodes in Neo4j."""
    try:
        with open(filepath, "r", encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            print(f"Unexpected JSON format in {filename}. Skipping...")
            return 0
        
        total_nodes = 0
        for record_idx, record in enumerate(data):
            # Create unique ID for the document
            doc_id = f"{filename}_{record_idx}"
            
            # Flatten and clean the record
            flat_record = flatten_dict(record)
            clean_record = {k: clean_property_value(v) for k, v in flat_record.items()}
            
            # Add metadata
            clean_record.update({
                "id": doc_id,
                "source_file": filename,
                "record_index": record_idx
            })
            
            # Create document node
            create_doc_query = """
            CREATE (d:Document)
            SET d = $properties
            RETURN d
            """
            session.run(create_doc_query, {"properties": clean_record})
            total_nodes += 1
            
            # Process long text fields into chunks
            for key, value in flat_record.items():
                if isinstance(value, str) and len(value) > CHUNK_SIZE:
                    chunks = chunk_text(value)
                    for chunk_idx, chunk_text in enumerate(chunks):
                        chunk_id = f"{doc_id}_{key}_{chunk_idx}"
                        chunk_props = {
                            "id": chunk_id,
                            "content": chunk_text,
                            "field_name": key,
                            "chunk_index": chunk_idx,
                            "total_chunks": len(chunks)
                        }
                        
                        # Create chunk node and relationship to document
                        create_chunk_query = """
                        MATCH (d:Document {id: $doc_id})
                        CREATE (c:Chunk)
                        SET c = $properties
                        CREATE (d)-[:HAS_CHUNK]->(c)
                        """
                        session.run(create_chunk_query, {
                            "doc_id": doc_id,
                            "properties": chunk_props
                        })
                        total_nodes += 1
        
        return total_nodes
    
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return 0

def main():
    try:
        # Connect to Neo4j
        driver = neo4j_client.get_driver()
        
        with driver.session() as session:
            # Create constraints
            create_constraints(session)
            
            total_nodes = 0
            files_processed = 0
            
            # Process each JSON file
            for file in os.listdir(JSON_DIR):
                if not file.endswith(".json"):
                    continue
                
                print(f"\nProcessing {file}...")
                filepath = os.path.join(JSON_DIR, file)
                nodes_created = process_json_file(filepath, file, session)
                
                total_nodes += nodes_created
                files_processed += 1
                print(f"Created {nodes_created} nodes for {file}")
            
            print(f"\n✅ Import complete!")
            print(f"📊 Files processed: {files_processed}")
            print(f"📈 Total nodes created: {total_nodes}")
    
    except Exception as e:
        print(f"❌ Error during import: {e}")
    finally:
        neo4j_client.disconnect()

if __name__ == "__main__":
    main()
