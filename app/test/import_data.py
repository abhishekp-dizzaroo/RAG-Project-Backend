# import json
# import weaviate
# from weaviate.collections.classes.config import DataType, Configure

# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# import pandas as pd
# from datetime import datetime
# from weaviate.util import generate_uuid5
# from tqdm import tqdm
# import hashlib
# from app.db.weaviate_client import weaviate_client
# client = weaviate_client.get_client()  

# json_dir = "json_files"

# # Configuration for chunking
# CHUNK_MARKERS = ["\n\n", "\n==", "\n##", "\n# ", "---", "\n\n---"]  # Common text separators
# MIN_CHUNK_SIZE = 50  # Minimum characters per chunk
# MAX_CHUNK_SIZE = 2000  # Maximum characters per chunk

# def normalize_record(record):
#     """
#     Normalize a single record to ensure consistent data types
#     """
#     normalized = {}
#     for key, value in record.items():
#         if isinstance(value, (list, dict)):
#             # Convert complex objects to JSON strings
#             normalized[key] = json.dumps(value)
#         elif value is None:
#             normalized[key] = ""
#         else:
#             normalized[key] = value
#     return normalized

# def infer_weaviate_type(value):
#     if isinstance(value, str):
#         try:
#             datetime.fromisoformat(value)
#             return "date"
#         except ValueError:
#             return "text"
#     elif isinstance(value, bool):
#         return "boolean"
#     elif isinstance(value, int):
#         return "int"
#     elif isinstance(value, float):
#         return "number"
#     elif isinstance(value, list):
#         if all(isinstance(i, int) for i in value):
#             return "int[]"
#         elif all(isinstance(i, float) for i in value):
#             return "number[]"
#         elif all(isinstance(i, str) for i in value):
#             return "text[]"
#         else:
#             return "text[]"
#     else:
#         return "text"

# def detect_headers_in_content(text):
#     """
#     Detect actual headers within the text content and create appropriate markers
#     """
#     if not isinstance(text, str):
#         return []
    
#     detected_markers = []
#     lines = text.split('\n')
    
#     for line in lines:
#         line_stripped = line.strip()
#         if not line_stripped:
#             continue
            
#         # Detect Markdown-style headers
#         if line_stripped.startswith('#'):
#             header_level = len(line_stripped) - len(line_stripped.lstrip('#'))
#             marker = f"\n{'#' * header_level} "
#             if marker not in detected_markers:
#                 detected_markers.append(marker)
        
#         # Detect underlined headers (=== or ---)
#         elif line_stripped.startswith('===') or line_stripped.startswith('---'):
#             if f"\n{line_stripped[:3]}" not in detected_markers:
#                 detected_markers.append(f"\n{line_stripped[:3]}")
        
#         # Detect potential headers (ALL CAPS lines, or lines ending with :)
#         elif (line_stripped.isupper() and len(line_stripped.split()) <= 6) or line_stripped.endswith(':'):
#             # Use the actual header text as marker
#             if f"\n{line_stripped}" not in detected_markers:
#                 detected_markers.append(f"\n{line_stripped}")
    
#     return detected_markers

# def chunk_text(text, base_markers=CHUNK_MARKERS):
#     """
#     Chunk text based on detected headers within the content and base markers
#     Returns list of chunks with metadata
#     """
#     if not isinstance(text, str) or len(text.strip()) == 0:
#         return [], "empty"
    
#     # First, detect headers within the actual content
#     content_markers = detect_headers_in_content(text)
    
#     # Combine detected markers with base markers, prioritizing content-specific ones
#     all_markers = content_markers + base_markers
    
#     best_chunks = []
#     best_marker = None
    
#     print(f"🔍 Detected headers in content: {content_markers}")
    
#     # Try each marker and find the one that creates the most reasonable chunks
#     for marker in all_markers:
#         chunks = text.split(marker)
#         # Filter out very small chunks
#         valid_chunks = [chunk.strip() for chunk in chunks if len(chunk.strip()) >= MIN_CHUNK_SIZE]
        
#         if len(valid_chunks) > len(best_chunks) and len(valid_chunks) > 1:
#             best_chunks = valid_chunks
#             best_marker = marker
    
#     # If no good chunking found, create chunks by character limit
#     if len(best_chunks) <= 1:
#         best_chunks = create_size_based_chunks(text)
#         best_marker = "size_based"
    
#     print(f"✂️ Using marker: {repr(best_marker)} - {len(best_chunks)} chunks created")
    
#     # Print first 3 chunks for verification
#     for i in range(min(3, len(best_chunks))):
#         chunk_preview = best_chunks[i][:100] + "..." if len(best_chunks[i]) > 100 else best_chunks[i]
#         print(f"   Chunk {i+1}: {repr(chunk_preview)}")
    
#     return best_chunks, best_marker

# def create_size_based_chunks(text, max_size=MAX_CHUNK_SIZE):
#     """
#     Create chunks based on size when no good markers are found
#     """
#     chunks = []
#     words = text.split()
#     current_chunk = []
#     current_size = 0
    
#     for word in words:
#         word_size = len(word) + 1  # +1 for space
#         if current_size + word_size > max_size and current_chunk:
#             chunks.append(' '.join(current_chunk))
#             current_chunk = [word]
#             current_size = word_size
#         else:
#             current_chunk.append(word)
#             current_size += word_size
    
#     if current_chunk:
#         chunks.append(' '.join(current_chunk))
    
#     return chunks

# def should_chunk_field(field_name, value):
#     if not isinstance(value, str):
#         return False
    
#     # Check if content is long enough to warrant chunking
#     if len(value) > MAX_CHUNK_SIZE:
#         return True
    
#     # Check if content contains potential headers (for shorter content)
#     if len(value) > MIN_CHUNK_SIZE:
#         # Look for header patterns in the content
#         if any(marker.strip() in value for marker in ['\n#', '\n##', '\n===', '\n---', ':']):
#             return True
    
#     return False

# def create_chunk_uuid(original_uuid, chunk_index):
#     chunk_id = f"{original_uuid}_{chunk_index}"
#     return generate_uuid5(chunk_id)

# def process_json_file(filepath, filename):
#     """
#     Process a single JSON file and return normalized records
#     """
#     try:
#         with open(filepath, "r", encoding='utf-8') as f:
#             data = json.load(f)
        
#         if not data:
#             print(f"Skipping empty file: {filename}")
#             return []
        
#         # Handle different JSON structures
#         if isinstance(data, dict):
#             # Single object
#             records = [data]
#         elif isinstance(data, list):
#             # Array of objects
#             records = data
#         else:
#             print(f"Unexpected JSON structure in {filename}: {type(data)}")
#             return []
        
#         # Normalize each record
#         normalized_records = []
#         for i, record in enumerate(records):
#             if not isinstance(record, dict):
#                 print(f"Skipping non-dict record {i} in {filename}: {type(record)}")
#                 continue
            
#             normalized = normalize_record(record)
#             normalized["source_file"] = filename
#             normalized["source_filename"] = os.path.splitext(filename)[0]
#             normalized["record_index"] = i
#             normalized_records.append(normalized)
        
#         print(f"📁 Processed {len(normalized_records)} records from {filename}")
#         return normalized_records
        
#     except json.JSONDecodeError as e:
#         print(f"❌ JSON decode error in {filename}: {e}")
#         return []
#     except Exception as e:
#         print(f"❌ Error processing {filename}: {e}")
#         return []

# # Step 1: Collect all data and infer unified schema
# all_data = []
# all_properties = {}
# COLLECTION_NAME = "RAG_PROJECT2"  # Single collection name

# print("🔍 Scanning all JSON files to build unified schema...")

# # Process each JSON file
# for file in os.listdir(json_dir):
#     if not file.endswith(".json"):
#         continue

#     filepath = os.path.join(json_dir, file)
#     records = process_json_file(filepath, file)
#     all_data.extend(records)

# if not all_data:
#     print("❌ No data found in any JSON files. Exiting.")
#     client.close()
#     sys.exit(1)

# print(f"📊 Total records collected: {len(all_data)}")

# # Build unified schema from all records
# print("🔧 Building unified schema...")
# for record in all_data:
#     for key, value in record.items():
#         if value is None or value == "":
#             continue
        
#         data_type = infer_weaviate_type(value)
        
#         # If property already exists, ensure compatibility
#         if key in all_properties:
#             if all_properties[key] != data_type:
#                 # Convert to text if types conflict
#                 all_properties[key] = "text"
#         else:
#             all_properties[key] = data_type

# # Convert to unified properties 
# def map_to_weaviate_enum(data_type):
#     mapping = {
#         "text": DataType.TEXT,
#         "date": DataType.DATE,
#         "int": DataType.INT,
#         "number": DataType.NUMBER,
#         "boolean": DataType.BOOL,
#         "text[]": DataType.TEXT_ARRAY,
#         "int[]": DataType.INT_ARRAY,
#         "number[]": DataType.NUMBER_ARRAY
#     }
#     return mapping.get(data_type, DataType.TEXT)

# properties = []
# for key, data_type in all_properties.items():
#     properties.append({"name": key, "data_type": map_to_weaviate_enum(data_type)})

# # Add metadata properties
# metadata_properties = [
#     {"name": "chunk_index", "data_type": DataType.INT},
#     {"name": "total_chunks", "data_type": DataType.INT},
#     {"name": "original_id", "data_type": DataType.TEXT},
#     {"name": "chunk_marker", "data_type": DataType.TEXT},
#     {"name": "is_chunked", "data_type": DataType.BOOL}
# ]

# properties.extend(metadata_properties)

# print(f"🔧 Unified schema with {len(properties)} properties")

# # Step 2: Create single collection with vectorizer configuration
# try:
#     if client.collections.exists(COLLECTION_NAME):
#         print(f"⚠️ Collection {COLLECTION_NAME} already exists - deleting and recreating...")
#         client.collections.delete(COLLECTION_NAME)
    
#     # Configure vectorizer and generative AI - choose one of these options:
    
#     # Option 1: Use OpenAI for both embeddings and generation (recommended)
#     vectorizer_config = Configure.Vectorizer.text2vec_openai()
#     generative_config = Configure.Generative.openai()
    
#     # Option 2: Use Cohere for both embeddings and generation
#     # vectorizer_config = Configure.Vectorizer.text2vec_cohere()
#     # generative_config = Configure.Generative.cohere()
    
#     # Option 3: Mix - OpenAI for generation, local embeddings
#     # vectorizer_config = Configure.Vectorizer.text2vec_transformers()
#     # generative_config = Configure.Generative.openai()
    
#     # Create collection with vectorizer and generative AI
#     client.collections.create(
#         name=COLLECTION_NAME, 
#         properties=properties,
#         vectorizer_config=vectorizer_config,
#         generative_config=generative_config
#     )
#     print(f"✅ Created unified collection with vectorizer: {COLLECTION_NAME}")
    
# except Exception as e:
#     print(f"❌ Error creating collection: {e}")
#     print("💡 Make sure you have the appropriate API keys configured in your Weaviate client")
#     client.close()
#     sys.exit(1)

# collection = client.collections.get(COLLECTION_NAME)

# # Step 3: Process all data and batch insert with chunking
# total_objects = 0

# print(f"\n🔄 Processing all {len(all_data)} records into single collection...")

# try:
#     with collection.batch.fixed_size(batch_size=100) as batch:
#         for i, record in tqdm(enumerate(all_data), total=len(all_data), desc=f"Processing {COLLECTION_NAME}"):
#             try:
#                 # Find fields that should be chunked
#                 chunked_fields = {}
#                 for key, value in record.items():
#                     if should_chunk_field(key, value):
#                         chunks, marker = chunk_text(value)
#                         if len(chunks) > 1:
#                             chunked_fields[key] = (chunks, marker)
#                             print(f"📄 Field '{key}' will be chunked using headers found in content")
                
#                 # Generate base UUID for this record
#                 unique_id = f"{record.get('source_file', 'unknown')}_{record.get('record_index', i)}_{hash(str(record))}"
#                 base_uuid = generate_uuid5(unique_id)
                
#                 if chunked_fields:
#                     # Process chunked version
#                     # Get the field with the most chunks (primary chunking field)
#                     primary_field = max(chunked_fields.keys(), key=lambda k: len(chunked_fields[k][0]))
#                     primary_chunks, primary_marker = chunked_fields[primary_field]
                    
#                     print(f"📝 Chunking record {i+1} from {record.get('source_file', 'unknown')}, field '{primary_field}' into {len(primary_chunks)} chunks")
                    
#                     for chunk_idx, chunk in enumerate(primary_chunks):
#                         obj = {}
                        
#                         # Copy all non-chunked fields
#                         for key, value in record.items():
#                             if key not in chunked_fields:
#                                 if isinstance(value, str):
#                                     try:
#                                         value = datetime.fromisoformat(value).isoformat()
#                                     except ValueError:
#                                         pass
#                                 obj[key] = value
                        
#                         # Add chunked content
#                         obj[primary_field] = chunk
                        
#                         # Handle other chunked fields (try to align with primary chunks)
#                         for field_name, (field_chunks, _) in chunked_fields.items():
#                             if field_name != primary_field:
#                                 if chunk_idx < len(field_chunks):
#                                     obj[field_name] = field_chunks[chunk_idx]
#                                 else:
#                                     obj[field_name] = ""  # Empty if no corresponding chunk
                        
#                         # Add chunk metadata
#                         obj["chunk_index"] = chunk_idx
#                         obj["total_chunks"] = len(primary_chunks)
#                         obj["original_id"] = str(base_uuid)
#                         obj["chunk_marker"] = primary_marker
#                         obj["is_chunked"] = True
                        
#                         chunk_uuid = create_chunk_uuid(base_uuid, chunk_idx)
#                         batch.add_object(properties=obj, uuid=chunk_uuid)
#                         total_objects += 1
                
#                 else:
#                     # Process as single object (no chunking needed)
#                     obj = {}
#                     for key, value in record.items():
#                         if isinstance(value, str):
#                             try:
#                                 value = datetime.fromisoformat(value).isoformat()
#                             except ValueError:
#                                 pass
#                         obj[key] = value
                    
#                     # Add chunk metadata for non-chunked objects
#                     obj["chunk_index"] = 0
#                     obj["total_chunks"] = 1
#                     obj["original_id"] = str(base_uuid)
#                     obj["chunk_marker"] = "none"
#                     obj["is_chunked"] = False
                    
#                     batch.add_object(properties=obj, uuid=base_uuid)
#                     total_objects += 1

#             except Exception as e:
#                 print(f"❌ Error processing record {i}: {e}")
#                 continue

#     print(f"✅ Processed {len(all_data)} records into {total_objects} objects in collection '{COLLECTION_NAME}'")

# except Exception as e:
#     print(f"❌ Error during batch processing: {e}")
# finally:
#     print("🎉 Processing completed!")
#     client.close()











import json
import weaviate
from weaviate.collections.classes.config import DataType, Configure
import sys
import os
import hashlib
from datetime import datetime
from weaviate.util import generate_uuid5
from tqdm import tqdm

# Import your existing Weaviate client setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.db.weaviate_client import weaviate_client

# Connect to Weaviate
client = weaviate_client.get_client()

# Config
json_dir = "json_files"
COLLECTION_NAME = "RAG_PROJECT_FLEXIBLE"
MIN_CHUNK_SIZE = 50
MAX_CHUNK_SIZE = 2000

#####################
# Utility Functions #
#####################

def flatten_dict(d, parent_key='', sep='.'):
    """Recursively flatten nested dictionaries."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def size_based_chunk(text, max_size=MAX_CHUNK_SIZE):
    """Split long text into size-based chunks."""
    words = text.split()
    chunks = []
    chunk = []
    current_size = 0

    for word in words:
        word_size = len(word) + 1
        if current_size + word_size > max_size and chunk:
            chunks.append(' '.join(chunk))
            chunk = [word]
            current_size = word_size
        else:
            chunk.append(word)
            current_size += word_size

    if chunk:
        chunks.append(' '.join(chunk))
    return chunks

def infer_weaviate_type(value):
    """Infer Weaviate data type for dynamic schema."""
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value)
            return "date"
        except ValueError:
            return "text"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "number"
    else:
        return "text"

def map_to_weaviate_enum(data_type):
    mapping = {
        "text": DataType.TEXT,
        "date": DataType.DATE,
        "int": DataType.INT,
        "number": DataType.NUMBER,
        "boolean": DataType.BOOL,
    }
    return mapping.get(data_type, DataType.TEXT)

#########################
# JSON File Processing  #
#########################

def process_json_file(filepath, filename):
    """Process one JSON file into flexible chunks."""
    records = []
    try:
        with open(filepath, "r", encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            print(f"Unexpected JSON format in {filename}. Skipping...")
            return []

        for i, record in enumerate(data):
            flat_record = flatten_dict(record)
            record_chunks = []

            for key, value in flat_record.items():
                if value is None:
                    value = ""
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                elif not isinstance(value, str):
                    value = str(value)

                if len(value) > MAX_CHUNK_SIZE:
                    sub_chunks = size_based_chunk(value)
                    for idx, chunk in enumerate(sub_chunks):
                        record_chunks.append({
                            "field": key,
                            "content": chunk,
                            "sub_chunk_index": idx,
                            "total_sub_chunks": len(sub_chunks)
                        })
                else:
                    record_chunks.append({
                        "field": key,
                        "content": value,
                        "sub_chunk_index": 0,
                        "total_sub_chunks": 1
                    })

            records.append({
                "source_file": filename,
                "record_index": i,
                "chunks": record_chunks
            })

        print(f"✅ Processed {len(records)} records from {filename}")
        return records

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")
        return []

##############################
# Build Unified Dynamic Schema
##############################

def build_unified_schema(all_records):
    property_types = {}

    for rec in all_records:
        for chunk in rec['chunks']:
            key = "field_name"
            value = chunk["field"]
            property_types[key] = infer_weaviate_type(value)

            key = "field_content"
            value = chunk["content"]
            property_types[key] = infer_weaviate_type(value)

    # Add common metadata fields
    property_types.update({
        "source_file": "text",
        "record_index": "int",
        "sub_chunk_index": "int",
        "total_sub_chunks": "int",
        "is_chunked": "boolean",
        "original_id": "text"
    })

    # Build property schema
    properties = []
    for key, dtype in property_types.items():
        properties.append({"name": key, "data_type": map_to_weaviate_enum(dtype)})

    return properties

##########################
# Create Collection Logic
##########################

def create_collection(properties):
    try:
        if client.collections.exists(COLLECTION_NAME):
            print(f"⚠️ Collection {COLLECTION_NAME} already exists - deleting...")
            client.collections.delete(COLLECTION_NAME)

        vectorizer_config = Configure.Vectorizer.text2vec_openai()  # Change this if needed
        generative_config = Configure.Generative.openai()

        client.collections.create(
            name=COLLECTION_NAME,
            properties=properties,
            vectorizer_config=vectorizer_config,
            generative_config=generative_config
        )
        print(f"✅ Created Weaviate collection: {COLLECTION_NAME}")
    except Exception as e:
        print(f"❌ Error creating collection: {e}")
        client.close()
        sys.exit(1)

#########################
# Main Ingestion Routine
#########################

def main():
    all_processed_data = []

    # Load and process all JSON files
    for file in os.listdir(json_dir):
        if not file.endswith(".json"):
            continue

        filepath = os.path.join(json_dir, file)
        file_records = process_json_file(filepath, file)
        all_processed_data.extend(file_records)

    if not all_processed_data:
        print("❌ No valid data found. Exiting.")
        client.close()
        sys.exit(1)

    print(f"\n📊 Total records processed: {len(all_processed_data)}")

    # Build unified schema
    properties = build_unified_schema(all_processed_data)
    create_collection(properties)
    collection = client.collections.get(COLLECTION_NAME)

    # Insert into Weaviate
    total_objects = 0
    print("\n🚀 Inserting data into Weaviate...")
    with collection.batch.fixed_size(batch_size=100) as batch:
        for rec in tqdm(all_processed_data):
            unique_id = f"{rec['source_file']}_{rec['record_index']}"
            base_uuid = generate_uuid5(unique_id)

            for chunk in rec['chunks']:
                obj = {
                    "source_file": rec['source_file'],
                    "record_index": rec['record_index'],
                    "field_name": chunk['field'],
                    "field_content": chunk['content'],
                    "sub_chunk_index": chunk['sub_chunk_index'],
                    "total_sub_chunks": chunk['total_sub_chunks'],
                    "is_chunked": chunk['total_sub_chunks'] > 1,
                    "original_id": str(base_uuid)
                }
                chunk_uuid = generate_uuid5(f"{base_uuid}_{chunk['field']}_{chunk['sub_chunk_index']}")
                batch.add_object(properties=obj, uuid=chunk_uuid)
                total_objects += 1

    print(f"✅ Successfully inserted {total_objects} objects into '{COLLECTION_NAME}' collection.")

    client.close()
    print("🎉 Processing complete!")

if __name__ == "__main__":
    main()
