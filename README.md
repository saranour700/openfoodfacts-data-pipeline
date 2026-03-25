# openfoodfacts-data-pipeline
# OpenFoodFacts Data Pipeline

This project is my attempt to build a complete data pipeline starting from raw data all the way to analytical insights.

I used the Open Food Facts dataset, which contains a large amount of real-world food data (ingredients, nutrition, categories, etc.), and tried to turn it into something clean and usable for analysis.

---

## Why I built this

I wanted to practice Data Engineering in a real scenario, not just small examples.

Instead of focusing on one tool, I tried to go through the full pipeline:

* working with messy real-world data
* cleaning and transforming it
* designing an analytical model
* and writing queries that answer real questions

---

## Pipeline Overview

```
Raw Data
   ↓
DuckDB (raw layer)
   ↓
dbt (silver layer - cleaning & transformation)
   ↓
dbt (gold layer - analytical model)
   ↓
SQL queries
```

---

## Project structure

```
openfoodfacts-data-pipeline/
│
├── notebooks/
│   └── exploration.ipynb
│
├── pipelines/
│   └── dlt_pipeline.py
│
├── off_dbt_project/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   │
│   └── dbt_project.yml
│
├── sql/
│   └── analytics.sql
│
└── README.md
```

---

## Data modeling approach

I followed a layered approach to separate concerns:

### Bronze

Raw data loaded into DuckDB without modification.

---

### Silver

In this layer, I focused on cleaning and standardizing the data:

* removed empty strings and replaced them with NULL
* handled missing values using COALESCE where appropriate
* converted timestamps to readable datetime
* flattened nested JSON fields (nutriments) into structured columns

Main models:

* silver_products
* silver_nutriments

---

### Gold

In this layer, I built an analytical model (star schema style).

* fact_products: contains all numerical metrics (energy, sugar, fat, etc.)
* dim_ingredients: exploded ingredient-level data for analysis

The goal here was to make the data easy to query and suitable for BI tools.

---

## Key design decisions

* Used DuckDB for local analytics because it is fast and lightweight
* Used dbt to manage transformations and dependencies between models
* Split transformations into silver and gold layers to keep logic organized
* Flattened JSON early to simplify downstream queries
* Used LEFT JOIN in the fact table to avoid losing products with missing nutrition data

---

## Example queries

```sql
-- Top brands by number of products
SELECT brands, COUNT(*) AS product_count
FROM gold.fact_products
GROUP BY brands
ORDER BY product_count DESC
LIMIT 10;
```

```sql
-- Products with highest sugar content
SELECT code, sugars_g
FROM gold.fact_products
ORDER BY sugars_g DESC
LIMIT 10;
```

---

## Tools used

* Python
* DuckDB
* dbt
* SQL

---

## How to run

```
source venv/bin/activate
dbt run
```

Then open DuckDB and run queries from the sql/analytics.sql file.

---

## What I learned

* How to work with messy, real-world datasets
* How to structure a data pipeline end-to-end
* How dbt manages transformations and dependencies
* How to design tables for analytical use cases

---

## Future improvements

* Add dbt tests for data quality
* Improve ingestion using dlt
* Build a dashboard for visualization

---

## About

Sara Nour

---

## Acknowledgment

Thanks to Open Food Facts for providing open data.
