from opensearchpy import OpenSearch, helpers
from datetime import datetime
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to https://localhost:9200 using SSL with verify_certs=False is insecure.")

client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "'Deeprecall@123'"),
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

def existing_doc_ids(index_name: str, doc_ids: list[str]) -> set[str]:
    if not client.indices.exists(index=index_name):
        return set()

    query = {
        "size": len(doc_ids),
        "_source": ["doc_id"],
        "query": {
            "terms": {
                "doc_id": doc_ids
            }
        }
    }

    res = client.search(index=index_name, body=query)
    return {hit["_source"]["doc_id"] for hit in res["hits"]["hits"]}


def input_metadata(docs: list[dict]):
    if not docs:
        raise ValueError("No documents provided")

    incoming_ids = [doc["doc_id"] for doc in docs if "doc_id" in doc]

    if len(incoming_ids) != len(docs):
        raise ValueError("Every document must contain a doc_id")

    duplicates = existing_doc_ids(INDEX, incoming_ids)
    if duplicates:
        raise ValueError(
            f"Duplicate doc_id(s) already exist in index '{INDEX}': {sorted(duplicates)}"
        )

    actions = []
    for doc in docs:
        doc["timestamp"] = datetime.utcnow()
        actions.append({
            "_op_type": "index",
            "_index": INDEX,
            "_id": doc["doc_id"],     # ENFORCE uniqueness
            "_source": doc
        })

    helpers.bulk(client, actions)
    print(f"Indexed {len(actions)} documents successfully")


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


def view_all_entries(index=INDEX, batch_size=1000):
    all_docs = []

    res = client.search(
        index=index,
        body={
            "query": {"match_all": {}}
        },
        size=batch_size,
        scroll="2m"
    )

    scroll_id = res["_scroll_id"]
    hits = res["hits"]["hits"]

    while hits:
        for hit in hits:
            all_docs.append(hit["_source"])

        res = client.scroll(
            scroll_id=scroll_id,
            scroll="2m"
        )
        scroll_id = res["_scroll_id"]
        hits = res["hits"]["hits"]

    client.clear_scroll(scroll_id=scroll_id)
    return all_docs

def view_doc_by_id(doc_id: str, index=INDEX):
    try:
        res = client.get(index=index, id=doc_id)
        return res["_source"]
    except Exception:
        return None

def empty_index(index_name=INDEX):
    if not client.indices.exists(index=index_name):
        return {
            "success": False,
            "error": "INDEX_NOT_FOUND",
            "message": f"Index '{index_name}' does not exist"
        }

    res = client.delete_by_query(
        index=index_name,
        body={
            "query": {
                "match_all": {}
            }
        },
        conflicts="proceed",
        refresh=True
    )

    return {
        "success": True,
        "deleted": res.get("deleted", 0),
        "index": index_name
    }


def delete_document_by_doc_id(
    doc_id: str,
    metadata_index="deeprecall-rc-fuzzy",
    vector_index="deeprecall-rc-vector"
):
    result = {
        "success": True,
        "doc_id": doc_id,
        "metadata_index": None,
        "vector_index": None
    }

    try:
        if client.indices.exists(index=metadata_index):
            res_meta = client.delete_by_query(
                index=metadata_index,
                body={
                    "query": {
                        "term": {
                            "doc_id": doc_id
                        }
                    }
                },
                conflicts="proceed",
                refresh=True
            )
            result["metadata_index"] = {
                "deleted": res_meta.get("deleted", 0),
                "index": metadata_index
            }
        else:
            result["metadata_index"] = {
                "deleted": 0,
                "index": metadata_index,
                "note": "index_not_found"
            }

        if client.indices.exists(index=vector_index):
            res_vec = client.delete_by_query(
                index=vector_index,
                body={
                    "query": {
                        "term": {
                            "doc_id": doc_id
                        }
                    }
                },
                conflicts="proceed",
                refresh=True
            )
            result["vector_index"] = {
                "deleted": res_vec.get("deleted", 0),
                "index": vector_index
            }
        else:
            result["vector_index"] = {
                "deleted": 0,
                "index": vector_index,
                "note": "index_not_found"
            }

    except Exception as e:
        result["success"] = False
        result["error"] = str(e)

    return result



if __name__ == "__main__":
    # all_entries = view_all_entries()
    # print(f"Total documents: {len(all_entries)}")

    # for doc in all_entries:
    #     print(doc)
    print(view_doc_by_id("test_doc_0011"))