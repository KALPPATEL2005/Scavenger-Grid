from sentence_transformers import SentenceTransformer
from typing import List
import torch

class SemanticEngine:
    def __init__(self):
        print("[*] Initializing Semantic Brain...")
        print("    Downloading/Loading model weights (all-MiniLM-L6-v2)...")
        # all-MiniLM is tiny (under 100MB) but highly effective
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        print("[+] Semantic Model loaded into memory successfully.")

    def process_text(self, text: str) -> List[float]:
        """Converts raw text into a dense mathematical vector array."""
        # The model returns a numpy array, we convert it to standard python floats
        # We explicitly tell it not to use a GPU so it doesn't crash on standard laptops
        with torch.no_grad():
            vector_array = self.model.encode(text, convert_to_numpy=True)
            
        vector = [float(x) for x in vector_array]
        return vector