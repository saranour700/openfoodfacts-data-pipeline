import duckdb
import requests
import json
import time
import os
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

class CanadianDataExtractor:
    def __init__(self, db_path: str = None):
        db_path = db_path or os.getenv("DUCKDB_PATH", "./canada_off.db")
        self.conn = duckdb.connect(db_path)
        self._setup_bronze_schema()
        
    def _setup_bronze_schema(self):
        """Create Bronze layer schema and tables"""
        self.conn.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
            
            CREATE TABLE IF NOT EXISTS bronze.raw_products (
                code VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                brands VARCHAR,
                countries_tags VARCHAR[],
                stores_tags VARCHAR[],
                ingredients_text VARCHAR,
                data JSON,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def extract_from_api_v2(self, limit: int = 1000) -> List[Dict]:
        """Extract Canadian products using OFF API v2 with pagination and streaming"""
        print(f"Extracting {limit} Canadian products...")
        
        all_products = []
        page = 1
        max_pages = (limit // 100) + 1
        
        while len(all_products) < limit and page <= max_pages:
            url = "https://world.openfoodfacts.org/api/v2/search"
            params = {
                "countries_tags_en": "canada",
                "fields": "code,product_name,brands,countries_tags,ingredients_text,stores_tags",
                "page": page,
                "page_size": 100
            }
            
            headers = {
                "User-Agent": "CanadianDB-Project/1.0 (contact@example.com)"
            }
            
            try:
                print(f"  Fetching page {page}...")
                response = requests.get(
                    url, 
                    params=params, 
                    headers=headers, 
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                products = data.get("products", [])
                if not products:
                    print("  No more products found")
                    break
                
                all_products.extend(products)
                print(f"  Got {len(products)} products (total: {len(all_products)})")
                page += 1
                
                time.sleep(0.5)
                
            except requests.exceptions.Timeout:
                print(f"  Timeout on page {page}, skipping...")
                break
            except Exception as e:
                print(f"  Error on page {page}: {e}")
                break
        
        print(f"Total Canadian products extracted: {len(all_products)}")
        return all_products[:limit]
    
    def save_to_bronze(self, products: List[Dict]) -> int:
        """Save extracted products to Bronze layer"""
        count = 0
        for product in products:
            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO bronze.raw_products 
                    (code, product_name, brands, countries_tags, stores_tags, ingredients_text, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    product.get("code"),
                    product.get("product_name"),
                    product.get("brands"),
                    product.get("countries_tags", []),
                    product.get("stores_tags", []),
                    product.get("ingredients_text"),
                    json.dumps(product)
                ])
                count += 1
            except Exception as e:
                print(f"  Error saving product {product.get('code')}: {e}")
        
        print(f"Saved {count} products to Bronze layer")
        return count
    
    def get_product_count(self) -> int:
        """Get total count of products in Bronze layer"""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM bronze.raw_products"
        ).fetchone()
        return result[0]
    
    def close(self):
        """Close database connection"""
        self.conn.close()


if __name__ == "__main__":
    extractor = CanadianDataExtractor()
    products = extractor.extract_from_api_v2(500)
    extractor.save_to_bronze(products)
    print(f"Database now has {extractor.get_product_count()} products")
    extractor.close()
