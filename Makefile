.PHONY: setup install extract transform test docs sync index search pipeline clean help

setup:
	python -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
	cd off_dbt_project && dbt deps

install:
	. venv/bin/activate && pip install -r requirements.txt
	cd off_dbt_project && dbt deps

extract:
	. venv/bin/activate && python extraction/off_extractor.py

transform:
	. venv/bin/activate && cd off_dbt_project && dbt run

test:
	. venv/bin/activate && cd off_dbt_project && dbt test

docs:
	. venv/bin/activate && cd off_dbt_project && dbt docs generate && dbt docs serve

sync:
	. venv/bin/activate && python sync/sync_manager.py

index:
	. venv/bin/activate && python -c "from vector_store.product_search import ProductVectorSearch; s = ProductVectorSearch(); s.index_from_duckdb()"

search:
	. venv/bin/activate && python -c "from vector_store.product_search import ProductVectorSearch; s = ProductVectorSearch(); print(s.search_similar('chocolate cookies'))"

pipeline: extract transform test

clean:
	rm -rf venv/
	rm -f canada_off.db
	rm -rf chroma_db/
	cd off_dbt_project && dbt clean

help:
	@echo "Available commands:"
	@echo "  make setup      - Create venv and install dependencies"
	@echo "  make extract    - Extract Canadian products from OFF API"
	@echo "  make transform  - Run dbt models (Silver & Gold)"
	@echo "  make test       - Run dbt data quality tests"
	@echo "  make docs       - Generate and serve dbt documentation"
	@echo "  make sync       - Run incremental sync"
	@echo "  make index      - Index products for vector search"
	@echo "  make search     - Search similar products"
	@echo "  make pipeline   - Full pipeline: extract + transform + test"
	@echo "  make clean      - Clean all generated files"
