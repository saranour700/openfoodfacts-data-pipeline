import chromadb
import json
import os
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

class ProductVectorSearch:
    def __init__(self, db_path: str = "./chroma_db"):
        self.client = chromadb.PersistentClient(path=db_path)
        self.collection = self.client.get_or_create_collection(
            name="canadian_products"
        )
    
    def index_from_duckdb(self, conn_string: str = None):
        """Index products from DuckDB to ChromaDB with duplicate check"""
        conn_string = conn_string or os.getenv("DUCKDB_PATH", "./canada_off.db")
        
        import duckdb
        conn = duckdb.connect(conn_string)
        
        existing_ids = set()
        try:
            existing = self.collection.get()
            if existing and existing['ids']:
                existing_ids = set(existing['ids'])
                print(f"Found {len(existing_ids)} existing products in index")
        except:
            pass
        
        products = conn.execute("""
            SELECT code, product_name, ingredients_text, brands 
            FROM bronze.raw_products 
            WHERE product_name IS NOT NULL
        """).fetchall()
        
        new_products = [(c, n, i, b) for c, n, i, b in products if c not in existing_ids]
        
        if not new_products:
            print("No new products to index")
            conn.close()
            return
        
        print(f"Indexing {len(new_products)} new products...")
        
        documents = []
        ids = []
        metadatas = []
        
        for code, name, ingredients, brand in new_products:
            doc = f"{name}. Ingredients: {ingredients or 'N/A'}"
            documents.append(doc)
            ids.append(code)
            metadatas.append({
                "brand": brand or "Unknown",
                "has_ingredients": ingredients is not None
            })
        
        batch_size = 100
        for i in range(0, len(documents), batch_size):
            end = i + batch_size
            self.collection.add(
                documents=documents[i:end],
                ids=ids[i:end],
                metadatas=metadatas[i:end]
            )
            print(f"  Indexed {min(end, len(documents))}/{len(documents)}")
        
        print(f"Indexing complete. Total in index: {len(existing_ids) + len(new_products)}")
        conn.close()
    
    def search_similar(self, query: str, n_results: int = 5) -> List[Dict]:
        """Search for similar products"""
        results = self.collection.query(
            query_texts=[query],
            n_results=n_results
        )
        
        products = []
        for i in range(len(results["ids"][0])):
            products.append({
                "code": results["ids"][0][i],
                "name": results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
                "distance": results["distances"][0][i] if "distances" in results else None
            })
        
        return products


if __name__ == "__main__":
    search = ProductVectorSearch()
    results = search.search_similar("chocolate chip cookies")
    for r in results:
        print(f"- {r['name']} (Code: {r['code']})")
