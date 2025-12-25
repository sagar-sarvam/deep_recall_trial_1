from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError
from sentence_transformers import SentenceTransformer

# Connect to OpenSearch
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


# Load embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

index_name = "vector-test"

# Check if index exists, create if not
if not client.indices.exists(index=index_name):
    client.indices.create(
        index=index_name,
        body={
            "mappings": {
                "properties": {
                    "text": {"type": "text"},
                    "embedding": {"type": "knn_vector", "dimension": 384}
                }
            }
        }
    )

# Passages to index
passages = [
    "On April 26, 1986, a routine safety test at Reactor No. 4 of the Chernobyl Nuclear Power Plant went catastrophically wrong. Engineers were attempting to simulate a power outage, but a combination of design flaws and human error triggered an uncontrollable power surge. The reactor core exploded, releasing massive amounts of radioactive materials into the atmosphere. The explosion blew off the reactor’s 1,000-ton steel and concrete lid, exposing the graphite moderator and fuel rods. A fire burned for ten days, releasing radioactive iodine, cesium, and strontium across vast regions of Europe. The nearby town of Pripyat, home to thousands of plant workers and their families, was not immediately evacuated. Residents went about their daily lives for 36 hours before receiving orders to leave, many never to return. Soviet authorities initially tried to contain information, but rising radiation levels in neighboring countries forced them to admit the disaster. Chernobyl became the worst nuclear accident in history, surpassing previous incidents by its scale, environmental impact, and human cost.",
    "In the immediate aftermath of the Chernobyl explosion, the Soviet government mobilized thousands of workers—later known as “liquidators”—to contain the disaster. These soldiers, engineers, and firefighters worked under intense radiation to extinguish fires, clear debris, and construct a massive concrete sarcophagus over the ruined reactor. Evacuation of Pripyat began on April 27, 1986, with over 49,000 residents transported by buses. They were told it would be temporary, but none ever returned. Within days, a 30-kilometer exclusion zone was established, and tens of thousands more people from surrounding villages were relocated. Radiation exposure caused acute health effects in plant workers and firefighters, many of whom died within weeks. Long-term exposure led to a spike in thyroid cancer, especially among children, and other health complications across Ukraine, Belarus, and Russia. Environmental contamination turned once-lively communities into ghost towns. The event also exposed the inefficiencies of Soviet crisis management, as delayed evacuation and secrecy worsened the disaster’s impact. Chernobyl’s aftermath marked a turning point in how the world viewed nuclear power safety and disaster response.",
    "Decades after the explosion, Chernobyl remains a haunting symbol of both technological ambition and human error. In 2016, a massive steel structure called the New Safe Confinement was placed over the old sarcophagus to further contain radiation from Reactor No. 4. Nature has slowly reclaimed the abandoned exclusion zone; forests, wild animals, and even rare species thrive in the absence of humans. Scientists continue to study the area to understand long-term radiation effects on ecosystems and human health. Chernobyl has also become a site of dark tourism, with guided visits to Pripyat and the power plant itself. While radiation levels remain hazardous in some areas, parts of the zone are safe enough for short visits. The disaster also inspired major policy changes—countries strengthened nuclear safety standards, and international agencies improved emergency coordination. Chernobyl’s story remains a powerful reminder of how a single accident can alter history, reshaping energy policy, international relations, and public trust in technology. Even today, its legacy echoes through environmental debates, scientific research, and cultural memory."
]

# Index each passage with unique ID
for idx, text in enumerate(passages, start=1):
    emb = model.encode(text).tolist()  # get embedding
    doc_id = f"passage_{idx}"          # unique ID
    try:
        client.index(
            index=index_name,
            id=doc_id,
            body={"text": text, "embedding": emb}
        )
        print(f"Indexed {doc_id}")
    except RequestError as e:
        print(f"Failed to index {doc_id}: {e}")

print("All passages indexed successfully.")