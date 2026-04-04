import sqlite3
import sqlite_vec
import json
import os

# Point to the new data directory
DB_FILE = "data/scavenger_vault.db"

class VectorVault:
    def __init__(self):
        print("[*] Initializing Master Vector Vault...")
        
        # Ensure the data directory exists
        os.makedirs("data", exist_ok=True)
        
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        
        # ENABLE WAL MODE for high-concurrency batch writes!
        self.conn.execute("PRAGMA journal_mode=WAL;")
        
        # Load the modern sqlite-vec extension into SQLite
        self.conn.enable_load_extension(True)
        sqlite_vec.load(self.conn)
        self.conn.enable_load_extension(False)
        
        self._create_tables()
        print("[+] Vault ready. Connected to local database.")

    def _create_tables(self):
        """Creates the tables if they don't exist yet."""
        cursor = self.conn.cursor()
        
        # Table 1: Stores the actual text chunks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT UNIQUE,
                content TEXT
            )
        """)
        
        # Table 2: The vec0 Virtual Table for storing the vectors
        # all-MiniLM-L6-v2 outputs exactly 384 dimensions
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS vec_documents USING vec0(
                vector float[384]
            )
        """)
        self.conn.commit()

    def insert_chunk(self, task_id: str, content: str, vector: list):
        """Saves a completed chunk and its vector into the vault."""
        cursor = self.conn.cursor()
        try:
            # 1. Insert the text into the standard table
            cursor.execute(
                "INSERT INTO documents (task_id, content) VALUES (?, ?)", 
                (task_id, content)
            )
            row_id = cursor.lastrowid
            
            # 2. Insert the mathematical vector into the vec table, linked by rowid
            # sqlite-vec seamlessly accepts JSON-formatted arrays
            vector_json = json.dumps(vector)
            cursor.execute(
                "INSERT INTO vec_documents (rowid, vector) VALUES (?, ?)", 
                (row_id, vector_json)
            )
            
            self.conn.commit()
            print(f"    [Vault] Saved Task {task_id[:8]}... to hard drive.")
            
        except sqlite3.IntegrityError:
            print(f"    [Vault] Task {task_id[:8]}... already exists in DB. Skipping.")