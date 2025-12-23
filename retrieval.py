from groq import Groq
import json
from fuzzy_metadata_search import fuzzy_search
from vectordb_functions import get_paras

client = Groq(api_key="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")

def extract_from_query(input_query: str) -> dict : 
    messages = [
        {
            "role": "system",
            "content": f"""
            You are a metadata extraction model.
            
            Your task is to extract key metadata fields from the given text and return a **JSON object**.
            Only include a field **if it is actually found or can be inferred with reasonable confidence** from the text.
            Do **not** include fields with null, empty, or unknown values.

            The possible metadata fields are:
            {{
            "scopeAndContent": "...",
            "generalNote": "...",
            "genreAccessPoints": ["..."],
            "culture": "...",
            "scientificTerms": ["..."],
            "entities": ["..."],
            "title": "...",
            "language": "...",
            "topics": ["..."],
            "geographic_locations": ["..."],
            "dates_mentioned": ["YYYY", "YYYY-MM", "YYYY-MM-DD"],
            "names_mentioned": ["..."],
            "places_mentioned": ["..."]
            }}

            Rules:
            - Only include fields that appear explicitly or are clearly implied.
            -always have field "scopeAndContent" with the scope of the querry
            - Omit missing or irrelevant fields entirely (even if it is an empty list).
            - Omit **empty lists** fields.
            - Use concise and factual extractions.
            - Always return **valid JSON** (no extra commentary or explanations).
            - Don't include any empty strings or empty lists. 

            Text:
            {input_query}
            """
        }
    ]

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        temperature=0.2,
        max_tokens=1000,
    )

    cont = response.choices[0].message.content
    return json.loads(cont)

def get_fvdict(raw_metadata):
    fv_dict = {}
    for key, value in raw_metadata.items():
        if isinstance(value, list):
            fv_dict[key] = ",".join(map(str, value))
        elif isinstance(value, str) and value.strip():
            fv_dict[key] = value.strip()
    
    return fv_dict

def final_paras(query,doc_id,index_name="deeprecall-rc-vector"):
    top_paras = get_paras(index_name,doc_id,query)
    final_content = ''
    for hit in top_paras["hits"]["hits"]:
        src = hit["_source"]
        #print(f"{src['text']}")
        final_content+=f"{src['text']}"
    return final_content


def retrieve(text):
    # raw_metadata = extract_from_query(text)
    print("hello i am here")
    fv_dict=get_fvdict(extract_from_query(text))
    print(fv_dict)
    doc_ids = fuzzy_search(fv_dict)
    contn = final_paras(text,doc_ids[0])
    return contn


if __name__ =="__main__":
    retrieve("Indus")

