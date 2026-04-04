# import sqlite3
# import sqlite_vec
# import json
# import torch
# from sentence_transformers import SentenceTransformer

# DB_FILE = "scavenger_vault.db"

# def search_vault():
#     print("[*] Loading AI Semantic Brain...")
#     # Load the EXACT SAME model the Worker used so the math matches
#     model = SentenceTransformer('all-MiniLM-L6-v2')
    
#     # Connect to the Vault
#     conn = sqlite3.connect(DB_FILE)
#     conn.enable_load_extension(True)
#     sqlite_vec.load(conn)
#     conn.enable_load_extension(False)
#     cursor = conn.cursor()

#     while True:
#         query = input("\n[?] Ask the Vault a question (or type 'exit'): ")
#         if query.lower() == 'exit':
#             break
            
#         # 1. Convert your question into a math vector
#         with torch.no_grad():
#             query_vector = model.encode(query, convert_to_numpy=True).tolist()
            
#         # 2. Ask SQLite to find the closest vectors in the database using L2 Distance
#         cursor.execute("""
#             SELECT d.content, vec_distance_L2(v.vector, ?) as distance
#             FROM vec_documents v
#             JOIN documents d ON v.rowid = d.rowid
#             ORDER BY distance ASC
#             LIMIT 2
#         """, (json.dumps(query_vector),))
        
#         results = cursor.fetchall()
        
#         print("\n=== TOP RESULTS ===")
#         for i, (content, distance) in enumerate(results):
#             # Lower distance = closer meaning
#             print(f"{i+1}. [Match Score: {distance:.4f}] {content}")

# if __name__ == "__main__":
#     print("=== Scavenger Grid Search Interface ===")
#     search_vault()


import sqlite3
import sqlite_vec
import json
import torch
import httpx
from sentence_transformers import SentenceTransformer

DB_FILE = "scavenger_vault.db"
OLLAMA_URL = "http://localhost:11434/api/generate"

def generate_answer(question: str, context: str):
    """Sends the found text and the user's question to the local LLM."""
    prompt = f"""You are the AI brain of the Scavenger Grid. 
Use ONLY the following context to answer the user's question. If the context doesn't contain the answer, say "I don't know based on the provided documents."

Context:
{context}

Question: {question}

Answer:"""

    print("\n[+] Grid Brain is thinking...")
    try:
        # Stream the response from local Ollama
        with httpx.stream("POST", OLLAMA_URL, json={"model": "llama3", "prompt": prompt}) as r:
            for chunk in r.iter_text():
                if chunk:
                    # Parse Ollama's JSON stream
                    data = json.loads(chunk)
                    print(data.get("response", ""), end="", flush=True)
        print("\n")
    except Exception as e:
        print(f"\n[!] Failed to connect to Ollama: {e}. Is Ollama running?")

def search_vault():
    print("[*] Loading Scavenger Grid Retrieval Engine...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    cursor = conn.cursor()

    while True:
        query = input("\n[?] Ask the Grid a question (or type 'exit'): ")
        if query.lower() == 'exit':
            break
            
        # 1. RETRIEVE: Find the closest paragraph in the local database
        with torch.no_grad():
            query_vector = model.encode(query, convert_to_numpy=True).tolist()
            
        cursor.execute("""
            SELECT d.content, vec_distance_L2(v.vector, ?) as distance
            FROM vec_documents v
            JOIN documents d ON v.rowid = d.rowid
            ORDER BY distance ASC
            LIMIT 1
        """, (json.dumps(query_vector),))
        
        result = cursor.fetchone()
        
        if result:
            best_match_text = result[0]
            print(f"    [Retrieved Context]: '{best_match_text[:75]}...'")
            
            # 2. GENERATE: Pass the text and the question to the LLM
            generate_answer(query, best_match_text)
        else:
            print("[!] The Vault is empty. Ingest some documents first!")

if __name__ == "__main__":
    print("=== Scavenger Grid RAG Interface ===")
    search_vault()