import requests
import os
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

class FoodDataCentralExtractor:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FOODDATA_API_KEY")
        self.base_url = "https://api.nal.usda.gov/fdc/v1"
        
        if not self.api_key:
            raise ValueError("FoodData API key required. Set FOODDATA_API_KEY env var.")
    
    def search_foods(self, query: str = "milk", page_size: int = 25) -> List[Dict]:
        """Search FoodData Central API"""
        url = f"{self.base_url}/foods/search"
        params = {
            "api_key": self.api_key,
            "query": query,
            "dataType": "Foundation,SR Legacy,Branded",
            "pageSize": page_size
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        foods = data.get("foods", [])
        
        print(f"Found {len(foods)} foods for query '{query}'")
        return foods
    
    def get_food_details(self, fdc_id: int) -> Dict:
        """Get detailed nutrition by FDC ID"""
        url = f"{self.base_url}/food/{fdc_id}"
        params = {"api_key": self.api_key}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    extractor = FoodDataCentralExtractor()
    results = extractor.search_foods("cheese", page_size=5)
    for food in results:
        print(f"- {food.get('description')} (FDC ID: {food.get('fdcId')})")
