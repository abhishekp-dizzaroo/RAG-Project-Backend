import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Property, DataType, Configure
from weaviate.util import generate_uuid5
import json
import os
from datetime import datetime

# --- Configuration ---
weaviate_url = os.environ.get("WEAVIATE_URL")
weaviate_api_key = os.environ.get("WEAVIATE_API_KEY")
openai_api_key = os.environ.get("OPENAI_API_KEY")

if not weaviate_url or not weaviate_api_key:
    print("WEAVIATE_URL and WEAVIATE_API_KEY environment variables must be set.")
    exit(1)

# --- Load JSON Data from File ---
json_file_path = "research_paper_data.json" # Or the full path to your file
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        paper_data = json.load(f)
except FileNotFoundError:
    print(f"Error: JSON file not found at {json_file_path}")
    exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from {json_file_path}. Please check its format.")
    exit(1)

# --- Weaviate Client Setup ---
client = weaviate.connect_to_weaviate_cloud(
    cluster_url=weaviate_url,
    auth_credentials=Auth.api_key(weaviate_api_key),
    headers={
        "X-OpenAI-Api-Key": openai_api_key
    }
)

# --- Define Weaviate Class (Schema) ---
class_name = "ResearchPaper"

if client.collections.exists(class_name):
    print(f"Collection '{class_name}' already exists. Skipping creation or deleting.")
    # client.collections.delete(class_name) # Uncomment to delete and recreate
    # print(f"Collection '{class_name}' deleted.")
else:
    print(f"Creating collection '{class_name}'...")
    client.collections.create(
        name=class_name,
        vectorizer_config=Configure.Vectorizer.text2vec_openai(),
        generative_config=Configure.Generative.openai(),
        properties=[
            Property(name="journal_name", data_type=DataType.TEXT),
            Property(name="publication_date", data_type=DataType.DATE),
            Property(name="volume", data_type=DataType.INT),
            Property(name="issue", data_type=DataType.INT),
            Property(name="article_title", data_type=DataType.TEXT, description="Title of the research paper"),
            Property(name="authors_list_short", data_type=DataType.TEXT_ARRAY),
            Property(name="contact_author_name", data_type=DataType.TEXT),
            Property(name="contact_author_email", data_type=DataType.TEXT),
            Property(name="contact_author_institution", data_type=DataType.TEXT),
            Property(name="doi", data_type=DataType.TEXT, skip_vectorization=True, tokenization=Configure.Tokenization.FIELD),
            Property(name="publication_info", data_type=DataType.TEXT),
            Property(name="copyright_text", data_type=DataType.TEXT),
            Property(name="page_range", data_type=DataType.TEXT),
            Property(name="cme_availability", data_type=DataType.TEXT),
            Property(name="usage_restriction", data_type=DataType.TEXT),
            Property(name="abstract_background", data_type=DataType.TEXT),
            Property(name="abstract_methods", data_type=DataType.TEXT),
            Property(name="abstract_results", data_type=DataType.TEXT),
            Property(name="abstract_conclusions", data_type=DataType.TEXT),
            Property(name="sections", data_type=DataType.OBJECT_ARRAY,
                     nested_properties=[
                         Property(name="title", data_type=DataType.TEXT),
                         Property(name="content", data_type=DataType.TEXT),
                     ]),
            Property(name="tables", data_type=DataType.OBJECT_ARRAY,
                     nested_properties=[
                         Property(name="table_id", data_type=DataType.TEXT, nameOverride="id"),
                         Property(name="caption", data_type=DataType.TEXT),
                         Property(name="columns", data_type=DataType.TEXT_ARRAY),
                         Property(name="rows", data_type=DataType.OBJECT_ARRAY, description="Array of row objects, each a dictionary."),
                         Property(name="footnotes", data_type=DataType.TEXT_ARRAY)
                     ]),
            Property(name="figures", data_type=DataType.OBJECT_ARRAY,
                     nested_properties=[
                         Property(name="figure_id", data_type=DataType.TEXT, nameOverride="id"),
                         Property(name="caption_general", data_type=DataType.TEXT),
                         Property(name="panels", data_type=DataType.OBJECT_ARRAY,
                                  nested_properties=[
                                      Property(name="panel_id", data_type=DataType.TEXT),
                                      Property(name="title", data_type=DataType.TEXT),
                                      Property(name="groups", data_type=DataType.OBJECT_ARRAY),
                                      Property(name="hazard_ratio_progression_death", data_type=DataType.TEXT, skip_vectorization=True),
                                      Property(name="hazard_ratio_death", data_type=DataType.TEXT, skip_vectorization=True),
                                      Property(name="p_value", data_type=DataType.TEXT, skip_vectorization=True),
                                      Property(name="x_axis_label", data_type=DataType.TEXT),
                                      Property(name="y_axis_label", data_type=DataType.TEXT),
                                      Property(name="time_points_months_no_at_risk", data_type=DataType.INT_ARRAY)
                                  ])
                     ]),
            Property(name="appendix_authors_full", data_type=DataType.OBJECT_ARRAY,
                     nested_properties=[
                         Property(name="author", data_type=DataType.TEXT),
                         Property(name="affiliation", data_type=DataType.TEXT)
                     ]),
            Property(name="appendix_principal_investigators_note", data_type=DataType.TEXT),
            Property(name="references", data_type=DataType.TEXT_ARRAY)
        ]
    )
    print(f"Collection '{class_name}' created successfully.")

# --- Prepare Data for Import ---
properties_to_add = {}
doc_meta = paper_data.get("document_metadata", {})
properties_to_add["journal_name"] = doc_meta.get("journal_name")
pub_date_str = doc_meta.get("publication_date")
if pub_date_str:
    try:
        dt_obj = datetime.strptime(pub_date_str, "%B %d, %Y")
        properties_to_add["publication_date"] = dt_obj.isoformat() + "Z"
    except ValueError:
        print(f"Warning: Could not parse publication_date: {pub_date_str}")
        properties_to_add["publication_date"] = None
properties_to_add["volume"] = doc_meta.get("volume")
properties_to_add["issue"] = doc_meta.get("issue")
properties_to_add["article_title"] = doc_meta.get("article_title")
properties_to_add["authors_list_short"] = doc_meta.get("authors_list_short")
contact_author = doc_meta.get("contact_author", {})
properties_to_add["contact_author_name"] = contact_author.get("name")
properties_to_add["contact_author_email"] = contact_author.get("email")
properties_to_add["contact_author_institution"] = contact_author.get("institution")
properties_to_add["doi"] = doc_meta.get("doi")
properties_to_add["publication_info"] = doc_meta.get("publication_info")
properties_to_add["copyright_text"] = doc_meta.get("copyright")
properties_to_add["page_range"] = doc_meta.get("page_range")
properties_to_add["cme_availability"] = doc_meta.get("cme_availability")
properties_to_add["usage_restriction"] = doc_meta.get("usage_restriction")

abstract_data = paper_data.get("abstract", {})
properties_to_add["abstract_background"] = abstract_data.get("background")
properties_to_add["abstract_methods"] = abstract_data.get("methods")
properties_to_add["abstract_results"] = abstract_data.get("results")
properties_to_add["abstract_conclusions"] = abstract_data.get("conclusions")

properties_to_add["sections"] = paper_data.get("sections", [])

tables_data = paper_data.get("tables", [])
processed_tables = []
for table in tables_data:
    processed_tables.append({
        "table_id": table.get("id"),
        "caption": table.get("caption"),
        "columns": table.get("columns"),
        "rows": table.get("rows", []),
        "footnotes": table.get("footnotes")
    })
properties_to_add["tables"] = processed_tables

figures_data = paper_data.get("figures", [])
processed_figures = []
for fig in figures_data:
    processed_panels = []
    for panel in fig.get("panels", []):
        processed_groups = []
        for group in panel.get("groups", []):
            no_at_risk = group.get("no_at_risk", [])
            try: no_at_risk_int = [int(x) for x in no_at_risk]
            except (ValueError, TypeError): no_at_risk_int = []
            processed_groups.append({
                "name": group.get("name"),
                "n_patients": group.get("n_patients"),
                "median_survival_months": group.get("median_survival_months"),
                "no_at_risk": no_at_risk_int
            })
        time_points = panel.get("time_points_months_no_at_risk", [])
        try: time_points_int = [int(x) for x in time_points]
        except (ValueError, TypeError): time_points_int = []
        processed_panels.append({
            "panel_id": panel.get("panel_id"),
            "title": panel.get("title"),
            "groups": processed_groups,
            "hazard_ratio_progression_death": panel.get("hazard_ratio_progression_death"),
            "hazard_ratio_death": panel.get("hazard_ratio_death"),
            "p_value": panel.get("p_value"),
            "x_axis_label": panel.get("x_axis_label"),
            "y_axis_label": panel.get("y_axis_label"),
            "time_points_months_no_at_risk": time_points_int
        })
    processed_figures.append({
        "figure_id": fig.get("id"),
        "caption_general": fig.get("caption_general"),
        "panels": processed_panels
    })
properties_to_add["figures"] = processed_figures

appendix_data = paper_data.get("appendix", {})
properties_to_add["appendix_authors_full"] = appendix_data.get("authors_full_list_and_affiliations", [])
properties_to_add["appendix_principal_investigators_note"] = appendix_data.get("principal_investigators_note")
properties_to_add["references"] = paper_data.get("references", [])

# --- Import Data ---
papers_collection = client.collections.get(class_name)

with papers_collection.batch.fixed_size(batch_size=1) as batch:
    paper_uuid = generate_uuid5(properties_to_add["doi"], class_name) if properties_to_add.get("doi") else None
    batch_return = batch.add_object(
        properties=properties_to_add,
        uuid=paper_uuid
    )
    if batch_return.has_errors:
        print(f"Error importing paper: {batch_return.errors}")

if papers_collection.batch.number_errors > 0:
    print(f"Batch import had {papers_collection.batch.number_errors} errors.")
    for failed_obj in papers_collection.batch.failed_objects:
        print(f"Failed object: {failed_obj.message}, Original: {failed_obj.object_}")
else:
    print("Paper data imported successfully (or no errors reported by batch).")

# --- Clean up ---
client.close()
print("Client closed.")