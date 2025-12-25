import os
import tempfile
from ocr_api_request import extract_text_from_pdf
from emergency_tag_extractor import extract_metadata
from fuzzy_metadata_search import input_metadata
from vectordb_functions import input_doc


def process_and_index_document(file, doc_id):
    try:
        print(f"Processing and indexing document {doc_id} from file {file}")

        ocr_op = extract_text_from_pdf(file)
        doc_text = ocr_op["extracted_text"]

        doc_metadata = extract_metadata(str(doc_text))
        doc_metadata["doc_id"] = doc_id

        input_metadata([doc_metadata])

        input_doc(
            doc_id,
            "deeprecall-rc-vector",
            doc_text,
            "all-MiniLM-L6-v2"
        )

        print(f"Document {doc_id} processed and indexed successfully.")
        return {
            "success": True,
            "doc_id": doc_id,
            "metadata": doc_metadata
        }

    except ValueError as e:
        if "already exist" in str(e):
            print(f"Duplicate doc_id detected: {doc_id}")
            return {
                "success": False,
                "error": "DUPLICATE_DOC_ID",
                "message": str(e),
                "doc_id": doc_id
            }
        else:
            raise

    except Exception as e:
        print(f"Failed to process document {doc_id}: {e}")
        return {
            "success": False,
            "error": "PROCESSING_FAILED",
            "message": str(e),
            "doc_id": doc_id
        }


# if __name__ == "__main__":
#     sample_pdf_path = 'demo.pdf'
#     sample_doc_id = 'doc_123'
#     process_and_index_document(sample_pdf_path, sample_doc_id)