import dlt
import requests
import gzip
import json
import os

@dlt.resource(name="products", write_disposition="merge", primary_key="code")
def get_products_delta():
    
    index_url = "https://static.openfoodfacts.org/data/delta/index.txt"
    response = requests.get(index_url)
    delta_files = response.text.strip().split("\n")

    latest_file = delta_files[0]
    file_url = f"https://static.openfoodfacts.org/data/delta/{latest_file}"
    local_path = f"data/raw/{latest_file}"

    print("Downloading file...")
    file_response = requests.get(file_url, stream=True, timeout=120)
    
    with open(local_path, "wb") as f:
        for chunk in file_response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print("Download complete, processing...")

    with gzip.open(local_path, "rb") as f:
        for line in f:
            product = json.loads(line)
            yield product

    os.remove(local_path)
    print("Done!")


pipeline = dlt.pipeline(
    pipeline_name="off_canada",
    destination="duckdb",
    dataset_name="bronze"
)

if __name__ == "__main__":
    load_info = pipeline.run(get_products_delta())
    print(load_info)