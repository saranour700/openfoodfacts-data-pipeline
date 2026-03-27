import duckdb
import requests
import json
import os
from datetime import datetime
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

class OFFSyncManager:
    def __init__(self, db_path: str = None):
        db_path = db_path or os.getenv("DUCKDB_PATH", "./canada_off.db")
        self.conn = duckdb.connect(db_path)
        self._setup_metadata_table()
        
    def _setup_metadata_table(self):
        """Create sync metadata tracking table"""
        self.conn.execute("""
            CREATE SCHEMA IF NOT EXISTS sync;
            
            CREATE TABLE IF NOT EXISTS sync.metadata (
                sync_id INTEGER PRIMARY KEY,
                last_sync TIMESTAMP,
                records_processed INTEGER,
                records_inserted INTEGER,
                records_updated INTEGER,
                status VARCHAR,
                error_message VARCHAR,
                sync_type VARCHAR DEFAULT 'incremental'
            )
        """)
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """Get last successful sync timestamp"""
        result = self.conn.execute("""
            SELECT MAX(last_sync) 
            FROM sync.metadata 
            WHERE status = 'success'
        """).fetchone()
        
        return result[0] if result and result[0] else None
    
    def incremental_sync(self, batch_size: int = 1000) -> Dict:
        """Sync only new/updated products since last sync"""
        last_sync = self.get_last_sync_time()
        
        print(f"Starting incremental sync...")
        print(f"   Last sync: {last_sync or 'Never'}")
        
        url = "https://world.openfoodfacts.org/api/v2/search"
        params = {
            "countries_tags_en": "canada",
            "fields": "code,product_name,brands,countries_tags,stores_tags,ingredients_text,last_modified_t",
            "sort_by": "last_modified_t",
            "page_size": batch_size
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            products = data.get("products", [])
            
            inserted = 0
            updated = 0
            
            for product in products:
                code = product.get("code")
                
                if not code:
                    continue
                
                existing = self.conn.execute("""
                    SELECT 1 FROM bronze.raw_products WHERE code = ?
                """, [code]).fetchone()
                
                if existing:
                    self.conn.execute("""
                        UPDATE bronze.raw_products 
                        SET product_name = ?,
                            brands = ?,
                            countries_tags = ?,
                            stores_tags = ?,
                            ingredients_text = ?,
                            data = ?,
                            imported_at = CURRENT_TIMESTAMP
                        WHERE code = ?
                    """, [
                        product.get("product_name"),
                        product.get("brands"),
                        product.get("countries_tags", []),
                        product.get("stores_tags", []),
                        product.get("ingredients_text"),
                        json.dumps(product),
                        code
                    ])
                    updated += 1
                else:
                    self.conn.execute("""
                        INSERT INTO bronze.raw_products 
                        (code, product_name, brands, countries_tags, stores_tags, ingredients_text, data)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [
                        code,
                        product.get("product_name"),
                        product.get("brands"),
                        product.get("countries_tags", []),
                        product.get("stores_tags", []),
                        product.get("ingredients_text"),
                        json.dumps(product)
                    ])
                    inserted += 1
            
            self.conn.execute("""
                INSERT INTO sync.metadata 
                (last_sync, records_processed, records_inserted, records_updated, status, sync_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                datetime.now(),
                len(products),
                inserted,
                updated,
                "success",
                "incremental"
            ])
            
            print(f"Sync completed: {inserted} inserted, {updated} updated")
            
            return {
                "status": "success",
                "processed": len(products),
                "inserted": inserted,
                "updated": updated
            }
            
        except Exception as e:
            self.conn.execute("""
                INSERT INTO sync.metadata 
                (last_sync, records_processed, status, error_message, sync_type)
                VALUES (?, ?, ?, ?, ?)
            """, [datetime.now(), 0, "failed", str(e), "incremental"])
            
            print(f"Sync failed: {e}")
            return {"status": "failed", "error": str(e)}
    
    def get_sync_history(self, limit: int = 10):
        """Get recent sync history"""
        return self.conn.execute("""
            SELECT * FROM sync.metadata 
            ORDER BY last_sync DESC 
            LIMIT ?
        """, [limit]).fetchdf()
    
    def full_refresh(self, extractor):
        """Full refresh - clear and reload"""
        print("Starting FULL REFRESH...")
        
        self.conn.execute("DELETE FROM bronze.raw_products")
        
        products = extractor.extract_from_api_v2(10000)
        count = extractor.save_to_bronze(products)
        
        self.conn.execute("""
            INSERT INTO sync.metadata 
            (last_sync, records_processed, records_inserted, status, sync_type)
            VALUES (?, ?, ?, ?, ?)
        """, [datetime.now(), count, count, "success", "full_refresh"])
        
        print(f"Full refresh completed: {count} products")
        return count


if __name__ == "__main__":
    from extraction.off_extractor import CanadianDataExtractor
    
    manager = OFFSyncManager()
    result = manager.incremental_sync()
    print(result)
    
    print("\nSync History:")
    print(manager.get_sync_history())
