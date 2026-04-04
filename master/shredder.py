import httpx
import sys
import os
import fitz  # PyMuPDF engine

MASTER_URL = "http://127.0.0.1:8000"

def chunk_text(text: str, chunk_size=150, overlap=30) -> list[str]:
    """
    Chops a massive string into overlapping chunks.
    Increased size to 150 words per chunk for better context from real PDFs.
    """
    words = text.split()
    chunks = []
    
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        if chunk.strip():
            chunks.append(chunk)
        
    return chunks

def extract_text_from_pdf(filepath: str) -> str:
    """Uses PyMuPDF to rapidly extract raw text from all pages."""
    text = ""
    with fitz.open(filepath) as doc:
        for page in doc:
            # Extract text and add a newline between pages
            text += page.get_text("text") + "\n"
    return text

def shred_file(filepath: str):
    print(f"[*] Reading {filepath}...")
    if not os.path.exists(filepath):
        print("[!] File not found.")
        return

    # 1. Determine the file type and extract content
    ext = os.path.splitext(filepath)[1].lower()
    content = ""
    
    if ext == ".txt":
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
    elif ext == ".pdf":
        print("[*] PDF detected. Engaging PyMuPDF extraction engine...")
        content = extract_text_from_pdf(filepath)
    else:
        print(f"[!] Unsupported file type: {ext}. Please provide a .txt or .pdf file.")
        return

    if not content.strip():
        print("[!] Document is empty or unreadable (might be an image-only PDF).")
        return

    # 2. Chop it up
    print("[*] Shredding document into semantic chunks...")
    chunks = chunk_text(content, chunk_size=150, overlap=30)
    
    # 3. Inject it into the Grid
    print(f"[*] Sending {len(chunks)} chunks to the Master Orchestrator...")
    try:
        response = httpx.post(f"{MASTER_URL}/ingest", json={"documents": chunks})
        response.raise_for_status()
        print(f"[+] Success! {response.json()}")
    except Exception as e:
        print(f"[!] Failed to send to Master: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python master/shredder.py <path_to_text_or_pdf_file>")
    else:
        shred_file(sys.argv[1])