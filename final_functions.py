import os
import tempfile
from ocr_api_request import extract_text_from_pdf
from emergency_tag_extractor import extract_metadata
from fuzzy_metadata_search import input_metadata
from vectordb_functions import input_doc


def process_and_index_document(file, doc_id):
    print(f"Processing and indexing document {doc_id} from file {file}")
    ocr_op = extract_text_from_pdf(file)
    doc_text = ocr_op['extracted_text']
    doc_metadata = extract_metadata(str(doc_text))
    doc_metadata["doc_id"] = doc_id
    input_metadata([doc_metadata])
    input_doc(doc_id, 'deeprecall-rc-vector', doc_text, 'all-MiniLM-L6-v2')
    print(f"Document {doc_id} processed and indexed successfully.")
    return doc_metadata

if __name__ == "__main__":
    sample_pdf_path = 'demo.pdf'
    sample_doc_id = 'doc_123'
    process_and_index_document(sample_pdf_path, sample_doc_id)