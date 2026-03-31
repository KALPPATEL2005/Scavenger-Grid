import sqlite3
import sqlite_vec
import json
import torch
from sentence_transformers import SentenceTransformer

DB_FILE = "scavenger_vault.db"

def search_vault():
    print("[*] Loading AI Semantic Brain...")
    # Load the EXACT SAME model the Worker used so the math matches
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Connect to the Vault
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    cursor = conn.cursor()

    while True:
        query = input("\n[?] Ask the Vault a question (or type 'exit'): ")
        if query.lower() == 'exit':
            break
            
        # 1. Convert your question into a math vector
        with torch.no_grad():
            query_vector = model.encode(query, convert_to_numpy=True).tolist()
            
        # 2. Ask SQLite to find the closest vectors in the database using L2 Distance
        cursor.execute("""
            SELECT d.content, vec_distance_L2(v.vector, ?) as distance
            FROM vec_documents v
            JOIN documents d ON v.rowid = d.rowid
            ORDER BY distance ASC
            LIMIT 2
        """, (json.dumps(query_vector),))
        
        results = cursor.fetchall()
        
        print("\n=== TOP RESULTS ===")
        for i, (content, distance) in enumerate(results):
            # Lower distance = closer meaning
            print(f"{i+1}. [Match Score: {distance:.4f}] {content}")

if __name__ == "__main__":
    print("=== Scavenger Grid Search Interface ===")
    search_vault()