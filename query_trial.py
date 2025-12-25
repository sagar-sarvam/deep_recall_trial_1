from opensearchpy import OpenSearch

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=("admin", "'Deeprecall@123'"),
    use_ssl=True,
    verify_certs=False
)

query_text = "which reactor exploded"

# Lexical search
response = client.search(
    index="vector-test",
    body={
        "size": 3,  # top 3 results
        "query": {
            "match": {
                "text": query_text
            }
        }
    }
)

# Print results
for hit in response['hits']['hits']:
    print(f"Score: {hit['_score']}")
    print(f"Text: {hit['_source']['text']}\n")