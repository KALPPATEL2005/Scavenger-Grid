import httpx
import sys
import os

MASTER_URL = "http://127.0.0.1:8000"

def chunk_text(text: str, chunk_size=300, overlap=50) -> list[str]:
    """
    Chops a massive string into overlapping chunks.
    Overlap prevents cutting an important sentence in half!
    """
    words = text.split()
    chunks = []
    
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
        
    return chunks

def shred_file(filepath: str):
    print(f"[*] Reading {filepath}...")
    if not os.path.exists(filepath):
        print("[!] File not found.")
        return

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    print("[*] Shredding document into semantic chunks...")
    chunks = chunk_text(content, chunk_size=50, overlap=10) # Small chunks for testing
    
    print(f"[*] Sending {len(chunks)} chunks to the Master Orchestrator...")
    try:
        response = httpx.post(f"{MASTER_URL}/ingest", json={"documents": chunks})
        response.raise_for_status()
        print(f"[+] Success! {response.json()}")
    except Exception as e:
        print(f"[!] Failed to send to Master: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python master/shredder.py <path_to_text_file>")
    else:
        shred_file(sys.argv[1])