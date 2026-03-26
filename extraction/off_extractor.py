import duckdb
import requests
import json
import gzip
from typing import List, Dict

class CanadianDataExtractor:
    def __init__(self, db_path: str = "canada_off.db"):
        self.conn = duckdb.connect(db_path)
        self._setup_bronze_schema()
        
    def _setup_bronze_schema(self):
        """Create Bronze layer tables"""
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
    
    def download_and_extract(self, limit: int = 5000) -> List[Dict]:
        """Download from OFF JSONL dump and filter Canadian"""
        url = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
        
        print(f"Downloading OFF dump...")
        print(f"URL: {url}")
        
        try:
            # Download
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            products = []
            count = 0
            canadian_count = 0
            
            # Stream and process
            import io
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                for line in gz:
                    if count >= limit * 3:  # Check more to find Canadian
                        break
                    
                    try:
                        product = json.loads(line.decode('utf-8'))
                        count += 1
                        
                        # Check if Canadian
                        countries = product.get("countries_tags", [])
                        if isinstance(countries, list):
                            if "en:canada" in countries or any("canada" in c.lower() for c in countries):
                                products.append(product)
                                canadian_count += 1
                                print(f"  Found Canadian: {product.get('product_name', 'N/A')[:50]} ({canadian_count})")
                                
                                if canadian_count >= limit:
                                    break
                        
                        if count % 10000 == 0:
                            print(f"  Scanned {count}, Found {canadian_count} Canadian...")
                            
                    except json.JSONDecodeError:
                        continue
            
            print(f"Scanned {count} total, found {len(products)} Canadian products")
            return products
            
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def save_to_bronze(self, products: List[Dict]) -> int:
        """Save raw data to Bronze layer"""
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
                print(f"  Error saving {product.get('code')}: {e}")
        
        print(f"Saved {count} products to Bronze layer")
        return count
    
    def get_product_count(self) -> int:
        result = self.conn.execute("SELECT COUNT(*) FROM bronze.raw_products").fetchone()
        return result[0]
    
    def close(self):
        self.conn.close()


if __name__ == "__main__":
    extractor = CanadianDataExtractor()
    products = extractor.download_and_extract(limit=1000)
    extractor.save_to_bronze(products)
    print(f"Total in database: {extractor.get_product_count()}")
    extractor.close()
