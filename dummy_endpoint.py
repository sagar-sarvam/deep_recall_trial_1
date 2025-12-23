from fastapi import FastAPI, UploadFile, File
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


@app.post("/process-text", response_model=TextOutput)
async def process_text(input_data: TextInput):
    user_query = input_data.text
    retrieved_text=retrieve(user_query)
    response= chat_with_llm(user_query,retrieved_text)
    return TextOutput(result=response)


@app.post("/process-file")
async def process_file(file: UploadFile = File(...), doc_id: Optional[str] = None):
    try:
        # Save file in current directory with a safe filename
        filename = file.filename or "uploaded_file"
        safe_filename = f"temp_{doc_id}_{filename}" if doc_id else f"temp_{filename}"
        file_path = os.path.join(os.getcwd(), safe_filename)
        
        # Save the file
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        print(f"File saved to {file_path}")
        
        # Process using relative path
        relative_path = os.path.basename(file_path)
        metadata = process_and_index_document(relative_path, doc_id)
     
        
        # Replace commas with spaces in list values
        processed_metadata = {}
        for key, value in metadata.items():
            if isinstance(value, list):
                processed_metadata[key] = ' '.join(map(str, value))
            else:
                processed_metadata[key] = value
        csv_content = json_to_csv(processed_metadata)

        return {
            "success": True,
            "csv": csv_content
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        # Clean up the temporary file
        if os.path.exists(file_path):
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
    uvicorn.run(app, host="127.0.0.1", port=8000)
