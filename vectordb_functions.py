'''from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError
from sentence_transformers import SentenceTransformer

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('admin', 'Deeprecall@123'),
    use_ssl=True,
    verify_certs=False
)


try:
    info = client.info()
    print("Connected to OpenSearch cluster:", info["cluster_name"])
except Exception as e:
    print("Connection failed:", e)
    exit(1)




def input_doc(doc_id, index_name, text, emb_model):        
    # Check if index exists, create if not
    global client
    model = SentenceTransformer(emb_model)
    if not client.indices.exists(index=index_name):
        client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "doc_id": {"type": "text"},
                        "page_no": {"type": "integer"},
                        "para_no": {"type": "integer"},
                        "text": {"type": "text"},
                        "embedding": {"type": "knn_vector", "dimension": 384}
                    }
                }
            }
        )


    chunks = []

    for pgs in text:
        paras = pgs.split("\n")
        chunks.append(paras)

    # Index each passage with unique ID
    for idx, text in enumerate(text, start=1):
        for pidx, para in enumerate(chunks[idx-1], start=1):
            emb = model.encode(para).tolist()  # get embedding
            id = f"{doc_id}_page_{idx}_para_{pidx}"          # unique para ID (docid + pageid + paraid)
            try:
                client.index(
                    index=index_name,
                    id=id,
                    body={"doc_id": doc_id, "page_no": idx, "para_no": pidx, "text": text, "embedding": emb}
                )
                print(f"Indexed {id}")
            except RequestError as e:
                print(f"Failed to index {id}: {e}")

            print(id, "indexed successfully.")


def get_paras(index_name, query_text, k=3):
    global client

    # Lexical search
    response = client.search(
        index=index_name,
        body={
            "size": k,  # top 3 results
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
        print(f"Text: {hit['_source']}\n")




if __name__ == "__main__":
    index_name = "vector-test-parawise"
    emb_model = 'all-MiniLM-L6-v2'
    doc_id = "doc_1"
    text = [
        "On April 26, 1986, a routine safety test at Reactor No. 4 of the Chernobyl Nuclear Power Plant went catastrophically wrong. Engineers were attempting to simulate a power outage, but a combination of design flaws and human error triggered an uncontrollable power surge. The reactor core exploded, releasing massive amounts of radioactive materials into the atmosphere. The explosion blew off the reactor’s 1,000-ton steel and concrete lid, exposing the graphite moderator and fuel rods. A fire burned for ten days, releasing radioactive iodine, cesium, and strontium across vast regions of Europe. The nearby town of Pripyat, home to thousands of plant workers and their families, was not immediately evacuated. Residents went about their daily lives for 36 hours before receiving orders to leave, many never to return. Soviet authorities initially tried to contain information, but rising radiation levels in neighboring countries forced them to admit the disaster. Chernobyl became the worst nuclear accident in history, surpassing previous incidents by its scale, environmental impact, and human cost.",
        "In the immediate aftermath of the Chernobyl explosion, the Soviet government mobilized thousands of workers—later known as “liquidators”—to contain the disaster. These soldiers, engineers, and firefighters worked under intense radiation to extinguish fires, clear debris, and construct a massive concrete sarcophagus over the ruined reactor. Evacuation of Pripyat began on April 27, 1986, with over 49,000 residents transported by buses. They were told it would be temporary, but none ever returned. Within days, a 30-kilometer exclusion zone was established, and tens of thousands more people from surrounding villages were relocated. Radiation exposure caused acute health effects in plant workers and firefighters, many of whom died within weeks. Long-term exposure led to a spike in thyroid cancer, especially among children, and other health complications across Ukraine, Belarus, and Russia. Environmental contamination turned once-lively communities into ghost towns. The event also exposed the inefficiencies of Soviet crisis management, as delayed evacuation and secrecy worsened the disaster’s impact. Chernobyl’s aftermath marked a turning point in how the world viewed nuclear power safety and disaster response.",
        "Decades after the explosion, Chernobyl remains a haunting symbol of both technological ambition and human error. In 2016, a massive steel structure called the New Safe Confinement was placed over the old sarcophagus to further contain radiation from Reactor No. 4. Nature has slowly reclaimed the abandoned exclusion zone; forests, wild animals, and even rare species thrive in the absence of humans. Scientists continue to study the area to understand long-term radiation effects on ecosystems and human health. Chernobyl has also become a site of dark tourism, with guided visits to Pripyat and the power plant itself. While radiation levels remain hazardous in some areas, parts of the zone are safe enough for short visits. The disaster also inspired major policy changes—countries strengthened nuclear safety standards, and international agencies improved emergency coordination. Chernobyl’s story remains a powerful reminder of how a single accident can alter history, reshaping energy policy, international relations, and public trust in technology. Even today, its legacy echoes through environmental debates, scientific research, and cultural memory."
    ]
    input_doc(doc_id, index_name, text, emb_model)
    print("All passages indexed successfully.")

    get_paras(index_name, "which government")
'''







from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError
from sentence_transformers import SentenceTransformer
from datetime import datetime


client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=("admin", "'Deeprecall@123'"),
    use_ssl=True,
    verify_certs=False
)

try:
    info = client.info()
    print("Connected to OpenSearch cluster:", info["cluster_name"])
except Exception as e:
    print("Connection failed:", e)
    exit(1)


def input_doc(doc_id, index_name, text, emb_model, metadata=None):
    global client
    model = SentenceTransformer(emb_model)

    if not client.indices.exists(index=index_name):
        client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "doc_id": {"type": "keyword"},
                        "page_no": {"type": "integer"},
                        "para_no": {"type": "integer"},
                        "embedding": {"type": "knn_vector", "dimension": 384}
                    }
                }
            }
        )
        print(f"Created index: {index_name}")

    chunks = [pg.split("\n") for pg in text]

    for idx, page_text in enumerate(text, start=1):
        for pidx, para in enumerate(chunks[idx - 1], start=1):
            emb = model.encode(para).tolist()
            unique_id = f"{doc_id}_page_{idx}_para_{pidx}"

            doc_body = {
                "doc_id": doc_id,
                "page_no": idx,
                "para_no": pidx,
                "text": para,
                "embedding": emb,
                "timestamp": datetime.now()
            }

            if metadata:
                doc_body.update(metadata)

            try:
                client.index(index=index_name, id=unique_id, body=doc_body)
                print(f"Indexed {unique_id}")
            except RequestError as e:
                print(f"Failed to index {unique_id}: {e}")


def get_paras(index_name, doc_ids, query_text=None, k=3):
    
    must_clauses = []

    if query_text:
        must_clauses.append({
            "match": {"text": {"query": query_text}}
        })
    print(doc_ids)
    if doc_ids:
        must_clauses.append({"term": {"doc_id": doc_ids}})

    query_body = {
        "size": k,
        "query": {
            "bool": {
                "must": must_clauses
            }
        }
    }

    response = client.search(index=index_name, body=query_body)
    
    print(f"\nTop {len(response['hits']['hits'])} Results:")
    for hit in response["hits"]["hits"]:
        src = hit["_source"]
        print(f"Score: {hit['_score']:.2f}")
        print(f"Doc ID: {src['doc_id']}, Page {src['page_no']}, Para {src['para_no']}")
        print(f"Text: {src['text'][:200]}...")
    return response

def clear_vector_index(index_name="deeprecall-rc-vector"):
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted successfully.")
    else:
        print(f"Index '{index_name}' does not exist.")


if __name__ == "__main__":
    index_name = "vector-test-parawise-meta"
    clear_vector_index("deeprecall-rc-vector")