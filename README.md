# OpenFoodFacts Canadian Database

An end-to-end **ELT data pipeline** that transforms raw OpenFoodFacts data into a clean, analytics-ready OLAP database focused on Canadian food products.

---

##  Overview

This project builds a complete **data engineering pipeline** that:

- Extracts data from OpenFoodFacts & external sources  
- Cleans and standardizes messy real-world data  
- Models it into a **star schema** for analytics  
- Enables fast OLAP queries using DuckDB  

The goal is to create a **high-quality Canadian food database** that can be used by other applications and data projects.

---

## Architecture
            ┌────────────────────┐
            │  Data Sources       │
            │---------------------│
            │ OpenFoodFacts API   │
            │ FoodData Central    │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │   Extraction Layer  │
            │ (Python + Requests)│
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │   Bronze Layer      │
            │   (DuckDB Raw)      │
            │---------------------│
            │ Raw JSON            │
            │ Sync metadata       │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │   Silver Layer      │
            │   (dbt Models)      │
            │---------------------│
            │ Cleaning            │
            │ Filtering (Canada)  │
            │ Flatten JSON        │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │    Gold Layer       │
            │   (Star Schema)     │
            │---------------------│
            │ fact_products       │
            │ dim_ingredients     │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │ Analytics Layer     │
            │---------------------│
            │ SQL Queries         │
            │ BI Tools            │
            │ Vector Search       │
            └────────────────────┘

---

## Data Pipeline Flow

1. Extract Canadian products from OpenFoodFacts API  
2. Store raw data in DuckDB (Bronze layer)  
3. Transform using dbt (Silver layer)  
4. Apply data quality tests  
5. Build analytical models (Gold layer)  
6. Run analytics queries  

---

##  Project Structureopenfoodfacts-data-pipeline/
│
├── extraction/ # Data extraction
│ ├── off_extractor.py
│ └── fooddata_extractor.py
│
├── sync/ # Incremental sync logic
│ └── sync_manager.py
│
├── vector_store/ # Similarity search (optional)
│ └── product_search.py
│
├── off_dbt_project/ # dbt transformations
│ ├── models/
│ │ ├── silver/
│ │ └── gold/
│ └── dbt_project.yml
│
├── notebooks/ # EDA & exploration
├── data/ # Local DuckDB storage
├── Makefile # Pipeline automation
├── README.md


---

##  Tech Stack

| Layer        | Tool        | Purpose |
|--------------|------------|--------|
| Extraction   | Python     | API ingestion |
| Storage      | DuckDB     | OLAP database |
| Transform    | dbt        | Data modeling & testing |
| Orchestration| Make       | Pipeline automation |
| Vector DB    | ChromaDB   | Similarity search |

---






