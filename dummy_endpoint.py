from fastapi import FastAPI, UploadFile, File,Form
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import csv
import tempfile
from retrieval import retrieve
import io
from typing import Optional, Dict, List, Any
from pydantic import BaseModel
import uvicorn
import json
from final_functions import process_and_index_document
from vectordb_functions import clear_vector_index
from fuzzy_metadata_search import existing_doc_ids, view_all_entries, view_doc_by_id, empty_index,delete_document_by_doc_id
from llm import chat_with_llm

app = FastAPI(title="Text & File Processing API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TextInput(BaseModel):
    text: str

class TextOutput(BaseModel):
    result: str

class FileProcessRequest(BaseModel):
    doc_id: str


@app.get("/entries")
async def get_all_entries():
    try:
        docs = view_all_entries()
        return {
            "success": True,
            "count": len(docs),
            "data": docs
        }
    except Exception as e:
        return {
            "success": False,
            "error": "FETCH_ALL_FAILED",
            "message": str(e)
        }

@app.delete("/documents/{doc_id}")
async def delete_document(doc_id: str):
    try:
        result = delete_document_by_doc_id(doc_id)
        return result

    except Exception as e:
        return {
            "success": False,
            "error": "DELETE_FAILED",
            "message": str(e),
            "doc_id": doc_id
        }


@app.get("/entries/{doc_id}")
async def get_entry_by_doc_id(doc_id: str):
    try:
        doc = view_doc_by_id(doc_id)

        if not doc:
            return {
                "success": False,
                "error": "NOT_FOUND",
                "message": f"Document with doc_id '{doc_id}' not found"
            }

        return {
            "success": True,
            "doc_id": doc_id,
            "data": doc
        }

    except Exception as e:
        return {
            "success": False,
            "error": "FETCH_FAILED",
            "message": str(e),
            "doc_id": doc_id
        }


@app.delete("/indexes/clean")
async def clean_indexes():
    result = {
        "success": True,
        "vector_index": None,
        "metadata_index": None
    }

    try:
        clear_vector_index("deeprecall-rc-vector")
        result["vector_index"] = {
            "action": "deleted",
            "index": "deeprecall-rc-vector"
        }
    except Exception as e:
        result["success"] = False
        result["vector_index"] = {
            "action": "failed",
            "error": str(e)
        }

    try:
        meta_res = empty_index("deeprecall-rc-fuzzy")
        result["metadata_index"] = meta_res
        if not meta_res.get("success", False):
            result["success"] = False
    except Exception as e:
        result["success"] = False
        result["metadata_index"] = {
            "action": "failed",
            "error": str(e)
        }

    return result


@app.post("/process-text", response_model=TextOutput)
async def process_text(input_data: TextInput):
    user_query = input_data.text
    retrieved_text=retrieve(user_query)
    response= chat_with_llm(user_query,retrieved_text)
    return TextOutput(result=response)


@app.post("/process-file")
async def process_file(
    file: UploadFile = File(...),
    doc_id: Optional[str] = Form(None)
):
    print("doc_id =", doc_id)
    duplicates = existing_doc_ids("deeprecall-rc-fuzzy", [doc_id])
    if duplicates:
        return {
            "success": False,
            "error": "DUPLICATE_DOC_ID",
            "message": f"doc_id '{doc_id}' already exists"
        }
    file_path = None

    try:
        filename = file.filename or "uploaded_file"
        safe_filename = f"temp_{doc_id}_{filename}" if doc_id else f"temp_{filename}"
        file_path = os.path.join(os.getcwd(), safe_filename)

        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)

        result = process_and_index_document(os.path.basename(file_path), doc_id)

        if not result.get("success"):
            return result

        processed_metadata = {}
        for key, value in result["metadata"].items():
            if isinstance(value, list):
                processed_metadata[key] = " ".join(map(str, value))
            else:
                processed_metadata[key] = value

        csv_content = json_to_csv(processed_metadata)

        return {
            "success": True,
            "doc_id": doc_id,
            "csv": csv_content
        }

    except Exception as e:
        return {
            "success": False,
            "error": "UPLOAD_FAILED",
            "message": str(e),
            "doc_id": doc_id
        }

    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def process_saved_file(file_path: str, doc_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        size = os.path.getsize(file_path)
    except Exception:
        size = 0
    
    return {
        "doc_id": doc_id or "unknown",
        "file_name": os.path.basename(file_path),
        "file_size": size,
        "status": "Processed"
    }


def json_to_csv(json_data: Dict[str, Any]) -> str:
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    
    headers = list(json_data.keys())
    writer.writerow(headers)
    
    # Convert lists to string representation for CSV
    values = []
    for value in json_data.values():
        if isinstance(value, list):
            values.append(', '.join(map(str, value)))
        else:
            values.append(str(value))
    
    writer.writerow(values)
    
    return csv_buffer.getvalue()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
