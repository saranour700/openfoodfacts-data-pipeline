<<<<<<< HEAD
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
=======
# openfoodfacts-data-pipeline

An end-to-end data engineering project that builds a full pipeline from raw food data to analytical insights using DuckDB and dbt.

---

## Overview

This project focuses on transforming raw OpenFoodFacts data into a structured analytical model that can be used for querying and insights.

The pipeline follows a layered architecture:

* Raw data ingestion
* Data cleaning and transformation
* Analytical modeling
* Querying for insights

---

## Project Goals

* Build a complete ELT pipeline
* Work with messy real-world data
* Design an OLAP-style schema
* Perform analytical queries
* Understand how dbt works in practice

---

## Architecture

```mermaid
flowchart TD
    A[Raw OpenFoodFacts Data] --> B[DuckDB - Bronze Layer]
    B --> C[dbt - Silver Layer]
    C --> D[dbt - Gold Layer]
    D --> E[SQL Analytics]
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
```

---

<<<<<<< HEAD
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
=======
## Data Pipeline Flow

```mermaid
flowchart LR
    R[Raw JSON Data] --> L[Load into DuckDB]
    L --> S[Silver Models - Cleaning & Transformation]
    S --> G[Gold Models - Fact & Dimensions]
    G --> Q[SQL Queries & Analytics]
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
```

---

<<<<<<< HEAD
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
=======
## Data Layers

### Bronze Layer

* Raw dataset loaded into DuckDB
* No transformations applied

---

### Silver Layer

Handles data cleaning and preparation:

* Removing empty values
* Standardizing fields
* Handling missing values
* Flattening JSON fields (nutriments)

Models:
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb

* silver_products
* silver_nutriments

---

<<<<<<< HEAD
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
=======
### Gold Layer

Analytical layer designed for querying:

* fact_products → main fact table with metrics
* dim_ingredients → ingredient-level analysis

This layer is optimized for analytical queries and BI tools.

---

## Features

* End-to-end data pipeline
* JSON data flattening
* Data cleaning and standardization
* Star schema modeling
* Analytical SQL queries

---

## Example Queries

```sql
-- Top brands by product count
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
SELECT brands, COUNT(*) AS product_count
FROM gold.fact_products
GROUP BY brands
ORDER BY product_count DESC
LIMIT 10;
```

```sql
<<<<<<< HEAD
-- Products with highest sugar content
=======
-- Products with highest sugar
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
SELECT code, sugars_g
FROM gold.fact_products
ORDER BY sugars_g DESC
LIMIT 10;
```

<<<<<<< HEAD
---

## Tools used
=======
```sql
-- Average energy per category
SELECT compared_to_category, AVG(energy_kcal)
FROM gold.fact_products
GROUP BY compared_to_category;
```

---

## Tech Stack
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb

* Python
* DuckDB
* dbt
* SQL

---

<<<<<<< HEAD
## How to run

```
=======
## How to Run

```bash
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
source venv/bin/activate
dbt run
```

<<<<<<< HEAD
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
=======
Then open DuckDB and execute queries from the sql folder.

---

## Future Improvements

* Add dbt tests for data validation
* Improve ingestion using dlt
* Build a dashboard for visualization
* Optimize transformations

---

## Author
>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb

Sara Nour

---

<<<<<<< HEAD
## Acknowledgment

Thanks to Open Food Facts for providing open data.
# openfoodfacts-data-pipeline
=======
## Notes

This project is part of my journey in learning Data Engineering and building real-world pipelines step by step.

>>>>>>> 391b2eeb84c824ce95be65d6a7cdab9b03e376cb
