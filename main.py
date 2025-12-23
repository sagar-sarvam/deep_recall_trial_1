from ocr_api_request import extract_text_from_pdf
from emergency_tag_extractor import extract_metadata
from fuzzy_metadata_search import *
from vectordb_functions import *
from llm import *
``
pdf_pth = 'demo.pdf'
result_json = extract_text_from_pdf(pdf_pth)
doc_id = 45
txt = result_json['extracted_text']
meta = extract_metadata(txt[0])
meta["doc_id"] = doc_id
input_metadata([meta])

input_doc(doc_id, 'deeprecall-rc-vector', txt[0],)

user_query = input('Enter your search query: ')

query = extract_metadata(user_query)

docs_to_search = fuzzy_search(query, k=3)
paras = get_paras(index_name, user_query, meta)

