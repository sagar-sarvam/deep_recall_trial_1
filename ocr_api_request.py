import requests

def extract_text_from_pdf(pdf_path):
    url = "https://62a34d3465520.notebooks.jarvislabs.net/proxy/8000/extract_text" 
    with open(pdf_path, "rb") as f:
        files = {"file": (pdf_path, f, "application/pdf")}
        response = requests.post(url, files=files)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    result = extract_text_from_pdf("/home/adi/files/deeprecall/demo.pdf")
    print(result)
