import chromadb
import json
from typing import List, Dict

class ProductVectorSearch:
    def __init__(self, db_path: str = "./chroma_db"):
        self.client = chromadb.PersistentClient(path=db_path)
        self.collection = self.client.get_or_create_collection(
            name="canadian_products"
        )
    
    def index_from_duckdb(self, conn_string: str = "canada_off.db"):
        """Index products from DuckDB to ChromaDB"""
        import duckdb
        
        conn = duckdb.connect(conn_string)
        products = conn.execute("""
            SELECT code, product_name, ingredients_text, brands 
            FROM bronze.raw_products 
            WHERE product_name IS NOT NULL
        """).fetchall()
        
        print(f"Indexing {len(products)} products...")
        
        documents = []
        ids = []
        metadatas = []
        
        for code, name, ingredients, brand in products:
            doc = f"{name}. Ingredients: {ingredients or 'N/A'}"
            documents.append(doc)
            ids.append(code)
            metadatas.append({
                "brand": brand or "Unknown",
                "has_ingredients": ingredients is not None
            })
        
        # Batch add
        batch_size = 100
        for i in range(0, len(documents), batch_size):
            end = i + batch_size
            self.collection.add(
                documents=documents[i:end],
                ids=ids[i:end],
                metadatas=metadatas[i:end]
            )
            print(f"  Indexed {min(end, len(documents))}/{len(documents)}")
        
        print("Indexing complete")
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
    # Test
    search = ProductVectorSearch()
    # search.index_from_duckdb()  # Run once to index
    results = search.search_similar("chocolate chip cookies")
    for r in results:
        print(f"- {r['name']} (Code: {r['code']})")
