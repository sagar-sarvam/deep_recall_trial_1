from opensearchpy import OpenSearch, helpers
from datetime import datetime
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to https://localhost:9200 using SSL with verify_certs=False is insecure.")

client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "Deeprecall@123"),
    use_ssl=True,
    verify_certs=False
)

try:
    info = client.info()
    print(f"Connected to OpenSearch cluster: {info['cluster_name']}")
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

INDEX = "deeprecall-rc-fuzzy"

settings = {
    "index": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

mappings = {
    "properties": {
        "doc_id": {"type": "keyword"},
        "scopeAndContent": {"type": "text"},
        "generalNote": {"type": "text"},
        "genreAccessPoints": {"type": "text"},
        "culture": {"type": "text"},
        "scientificTerms": {"type": "text"},
        "entities": {"type": "text"},
        "title": {"type": "text"},
        "language": {"type": "text"},
        "topics": {"type": "text"},
        "geographic_locations": {"type": "text"},
        "dates_mentioned": {"type": "text"},
        "names_mentioned": {"type": "text"},
        "places_mentioned": {"type": "text"},
        "timestamp": {"type": "date"}
    }
}


if not client.indices.exists(index=INDEX):
    client.indices.create(index=INDEX, body={"settings": settings, "mappings": mappings})
    print(f"Created index: {INDEX}")
else:
    print(f"Index '{INDEX}' already exists.")


def input_metadata(docs):
    actions = []
    for doc in docs:
        doc["timestamp"] = datetime.utcnow()
        actions.append({
            "_op_type": "index",
            "_index": INDEX,
            "_id": doc.get("doc_id"),
            "_source": doc
        })
    helpers.bulk(client, actions)
    print(f"Indexed {len(docs)} documents with metadata")


def fuzzy_search(fv_dict, index=INDEX, k=5):
    match_count = {}

    for field, value in fv_dict.items():
        query_body = {
            "query": {
                "match": {
                    field: {
                        "query": value,
                        "fuzziness": "AUTO",
                        "prefix_length": 1,
                        "max_expansions": 50
                    }
                }
            }
        }

        res = client.search(index=index, body=query_body)
        for hit in res["hits"]["hits"]:
            doc_id = hit["_source"].get("doc_id", "unknown_id")
            match_count[doc_id] = match_count.get(doc_id, 0) + 1

    if not match_count:
        print("No matches found for any metadata fields.")
        return []

    sorted_match_count = sorted(match_count.items(), key=lambda x: x[1], reverse=True)
    top_docs = [doc_id for doc_id, _ in sorted_match_count[:k]]

    print(f"Top {len(top_docs)} documents: {top_docs}")
    return top_docs



if __name__ == "__main__":
    docs = [
        {
            "doc_id": "1",
            "title": "Chernobyl disaster response report",
            "language": "English",
            "topics": "nuclear disaster, emergency response",
            "geographic_locations": "Ukraine",
            "dates_mentioned": "1986",
            "names_mentioned": "Valery Legasov",
            "scientific_terms": "radiation, graphite reactor",
            "final_summary": "An analysis of how the Chernobyl disaster was handled."
        },
        {
            "doc_id": "2",
            "title": "Natural Language Processing with Python",
            "language": "English",
            "topics": "machine learning, NLP",
            "scientific_terms": "tokenization, stemming",
            "final_summary": "An introduction to processing natural language using Python."
        }
    ]

    input_metadata(docs)

    query = {"topics": "disaster"}
    top_docs = fuzzy_search(query, INDEX, k=2)
    print("Top matching documents:", top_docs)
