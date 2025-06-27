import weaviate
import weaviate.classes.config as wvc
import os
import json
from tqdm import tqdm
import warnings
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
COLLECTION_NAME = "Medical_Paper_RAG"
JSON_FILE_PATH = "./json_files/NEJMoa2203690.json"

# --- 1. Connect to Weaviate ---
def setup_weaviate_client():
    """Sets up and returns a Weaviate client."""
    if not all([WEAVIATE_URL, WEAVIATE_API_KEY, OPENAI_API_KEY]):
        raise ValueError("Please set WEAVIATE_URL, WEAVIATE_API_KEY, and OPENAI_API_KEY environment variables.")
    
    print("Connecting to Weaviate...")
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=weaviate.auth.AuthApiKey(WEAVIATE_API_KEY),
        headers={"X-OpenAI-Api-Key": OPENAI_API_KEY},
        skip_init_checks=True
    )
    print(f"Connection successful. Weaviate is ready.")
    return client

# --- 2. Define and Create Collection Schema ---
def create_collection_schema(client: weaviate.WeaviateClient):
    """Defines and creates the collection in Weaviate."""
    if client.collections.exists(COLLECTION_NAME):
        print(f"Collection '{COLLECTION_NAME}' already exists. Deleting and recreating.")
        client.collections.delete(COLLECTION_NAME)

    print(f"Creating collection '{COLLECTION_NAME}'...")
    collection = client.collections.create(
        name=COLLECTION_NAME,
        vectorizer_config=wvc.Configure.Vectorizer.text2vec_openai(),
        generative_config=wvc.Configure.Generative.openai(),
        properties=[
            wvc.Property(name="content", data_type=wvc.DataType.TEXT),
            wvc.Property(name="chunk_type", data_type=wvc.DataType.TEXT),
            wvc.Property(name="document_title", data_type=wvc.DataType.TEXT),
            wvc.Property(name="parent_id", data_type=wvc.DataType.TEXT),
            wvc.Property(name="section_title", data_type=wvc.DataType.TEXT, skip_vectorization=True),
            wvc.Property(name="table_id", data_type=wvc.DataType.TEXT, skip_vectorization=True),
            wvc.Property(name="figure_id", data_type=wvc.DataType.TEXT, skip_vectorization=True),
            wvc.Property(name="row_index", data_type=wvc.DataType.INT, skip_vectorization=True),
            wvc.Property(name="source_file", data_type=wvc.DataType.TEXT, skip_vectorization=True),
            wvc.Property(name="doi", data_type=wvc.DataType.TEXT, skip_vectorization=True),
        ]
    )
    print("Collection created successfully.")
    return collection

# --- 3. Helper function for formatting table data ---
def format_table_row_content(row_data: dict, columns: list) -> str:
    """Creates a self-contained, readable string for a table row."""
    parts = []
    # The first column is the 'Variable' or 'Characteristic', which is the primary key for the row's meaning.
    row_header = list(row_data.values())[0]
    parts.append(f"Row: '{row_header}'")

    # Iterate through the rest of the columns and their corresponding values in the row
    for col_name, cell_value in list(zip(columns, row_data.values()))[1:]:
        if cell_value is not None and str(cell_value).strip() != "":
            # Clean up complex column names for better readability
            clean_col_name = col_name.replace('_', ' ').replace('Cohort', '').strip()
            parts.append(f"{clean_col_name}: {cell_value}")
    return " | ".join(parts)

# --- 4. Main Ingestion Logic ---
def ingest_medical_paper(client: weaviate.WeaviateClient, filepath: str):
    """Reads, chunks, and ingests a medical paper from a JSON file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"The file {filepath} was not found.")

    with open(filepath, 'r', encoding='utf-8') as f:
        doc = json.load(f)

    collection = client.collections.get(COLLECTION_NAME)
    
    print(f"\nStarting ingestion for '{os.path.basename(filepath)}'...")
    with collection.batch.dynamic() as batch:
        # --- Document Metadata ---
        doc_meta = doc.get('document_metadata', {})
        doc_title = doc_meta.get('article_title', 'Unknown Title')
        doc_doi = doc_meta.get('doi', 'Unknown DOI')
        source_file = os.path.basename(filepath)
        parent_uuid = weaviate.util.generate_uuid5(doc_doi or doc_title)

        # --- Ingest Sections ---
        for section in tqdm(doc.get('sections', []), desc="Processing Sections"):
            section_title = section.get('title')
            if section.get('content'):
                batch.add_object(
                    properties={
                        "content": f"Section: {section_title}\n\n{section.get('content')}",
                        "chunk_type": "section", "document_title": doc_title, "parent_id": parent_uuid,
                        "section_title": section_title, "source_file": source_file, "doi": doc_doi,
                    },
                    uuid=weaviate.util.generate_uuid5(f"{parent_uuid}_section_{section_title}")
                )
            for subsection in section.get('subsections', []):
                subsection_title = subsection.get('title')
                full_title = f"{section_title}.{subsection_title}"
                if subsection.get('content'):
                    batch.add_object(
                        properties={
                            "content": f"Section: {full_title}\n\n{subsection.get('content')}",
                            "chunk_type": "section", "document_title": doc_title, "parent_id": parent_uuid,
                            "section_title": full_title, "source_file": source_file, "doi": doc_doi,
                        },
                        uuid=weaviate.util.generate_uuid5(f"{parent_uuid}_section_{full_title}")
                    )

        # --- Ingest Tables (Row by Row) ---
        for table in tqdm(doc.get('tables', []), desc="Processing Tables"):
            table_id = table.get('id')
            table_caption = table.get('caption', '')
            columns = table.get('columns', [])
            for i, row in enumerate(table.get('rows', [])):
                row_content = format_table_row_content(row, columns)
                full_content = f"Data from {table_id} (Caption: {table_caption}):\n{row_content}"
                batch.add_object(
                    properties={
                        "content": full_content, "chunk_type": "table_row", "document_title": doc_title,
                        "parent_id": parent_uuid, "table_id": table_id, "row_index": i,
                        "source_file": source_file, "doi": doc_doi,
                    },
                    uuid=weaviate.util.generate_uuid5(f"{parent_uuid}_{table_id}_row_{i}")
                )

        # --- Ingest Figure Captions ---
        for figure in tqdm(doc.get('figures', []), desc="Processing Figures"):
            figure_id = figure.get('id')
            caption = figure.get('caption_general', '')
            if caption:
                batch.add_object(
                    properties={
                        "content": f"Caption for {figure_id}: {caption}", "chunk_type": "figure_caption",
                        "document_title": doc_title, "parent_id": parent_uuid, "figure_id": figure_id,
                        "source_file": source_file, "doi": doc_doi,
                    },
                    uuid=weaviate.util.generate_uuid5(f"{parent_uuid}_{figure_id}")
                )
    
    # The batch manager automatically sends the data, so we just need a confirmation message.
    print(f"\nBatch ingestion process finished.")

# --- 5. Demonstration Query ---
def run_query_example(client: weaviate.WeaviateClient):
    """Runs a sample query to demonstrate the system's accuracy."""
    print("\n--- Running Demonstration Query ---")
    
    collection = client.collections.get(COLLECTION_NAME)
    
    # This is a complex question that requires finding a specific value in a specific table column.
    query = "What was the median progression-free survival in the hormone receptor-positive cohort for patients taking Trastuzumab Deruxtecan?"
    
    print(f"❓ QUERY: {query}\n")
    
    # Use a 'where' filter to specifically target the most relevant data type (table rows)
    response = collection.generate.near_text(
        query=query,
        limit=3, # Retrieve the top 3 most relevant table rows
        filters=wvc.Filter.by_property("chunk_type").equal("table_row"),
        single_prompt="Based on the following context, answer the question: {query}\n\nContext:\n{content}\n\nAnswer:"
    )
    
    print(f"✅ GENERATED ANSWER:\n{response.generated}\n")
    print("--- Retrieved Context Used for Generation ---")
    for obj in response.objects:
        print(f"CHUNK (Table: {obj.properties['table_id']}, Row: {obj.properties['row_index']}):")
        print(f"  -> Content: {obj.properties['content']}")
        print("-" * 20)

# --- Main Execution Block ---
if __name__ == "__main__":
    # The following line is commented out to prevent errors on older versions of the weaviate-client.
    # It is not essential for the script's functionality.
    # warnings.filterwarnings("ignore", category=weaviate.WeaviateExperimentalWarning)
    
    try:
        client = setup_weaviate_client()
        create_collection_schema(client)
        ingest_medical_paper(client, JSON_FILE_PATH)
        run_query_example(client)
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if 'client' in locals() and client.is_connected():
            client.close()
            print("\nWeaviate client closed.")
