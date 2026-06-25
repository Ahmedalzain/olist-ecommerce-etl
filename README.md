# Olist E-Commerce Data Pipeline & Analysis Project

This project encompasses a data engineering pipeline designed to process and analyze Brazilian e-commerce data from **Olist** — covering ~100,000 orders, customers, products, and sellers between 2016 and 2023. The processed data provides insights into customer behavior, product performance, and order fulfillment across multiple dimensions.

---

## Architecture Diagram

![Olist Data Warehouse ERD](docs/erd_diagram.png)

---

## Tech Stack & Tools

- **Data Processing**: Apache Spark (PySpark)
- **Data Warehouse / Database**: PostgreSQL
- **ETL Scripts**: Python
- **Source Data**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle)

---

## Pipeline Overview

The pipeline starts by ingesting raw data from CSV files using **Apache Spark** for distributed data processing. Following the ETL (Extract, Transform, Load) process, the data is cleaned, deduplicated, and restructured into a star schema, then stored in **PostgreSQL** under the `dwh` schema, which acts as the central data warehouse layer.

The pipeline is split into three independent stages — `etl_scripts/extract.py`, `etl_scripts/transform.py`, and `etl_scripts/load.py` — orchestrated by a single entry point, `etl_scripts/main.py`.

---

## Getting Started

### Prerequisites
- Python 3.11+
- Java (required by Spark)
- PostgreSQL running locally or remotely

### Installation & Setup

1. Clone the repo
2. Install dependencies:
```bash
   pip install pyspark psycopg2-binary
```
3. Place the raw Olist CSV files inside `source_data/`
4. Set the required environment variables:
```bash
   export DB_PASSWORD="your_postgres_password"
   export RUN_DB_LOAD="true"
```
5. Run the pipeline:
```bash
   python etl_scripts/main.py
```

---

## Data Extraction from Source Files

The data extraction process involves reading raw CSV files for **Orders, Order Items, Customers, Products, and Sellers**. Each file is loaded as a Spark DataFrame in `extract.py`, with row counts logged for traceability.

To extract the data, Spark's native CSV reader is used with `inferSchema=True`. Extraction is isolated from all transformation logic, keeping I/O concerns separate from business rules.

---

## Transformation and Loading into the Data Warehouse Schema

Once the data is extracted, transformation routines (`transform.py`) are applied to clean, normalize, and enrich the datasets. These transformations include:

- Standardizing timestamp formats (`order_purchase_timestamp` → real `timestamp` type)
- Casting numeric fields (`price`, `freight_value`) to `double`
- Deduplicating records per natural key (`customer_id`, `product_id`, `seller_id`)
- Normalizing text fields (city/state names, category names) via `UPPER`/`TRIM`
- Generating surrogate keys for each dimension
- Joining the fact table against all four dimensions

The transformed data is then loaded (`load.py`) into a structured **Star Schema** within PostgreSQL's `dwh` schema, optimized for analytical queries.

---

## Star Schema Overview

The **Star Schema** is employed in this data warehouse to keep analytical queries simple and performant — each dimension joins directly to the fact table in a single hop, without intermediate snowflaking.

### Fact Table

- **FactOrderItems** (`fact_order_items`): Captures one row per order item — price, freight value, order status, and purchase date — linked to all four dimensions via surrogate keys.

### Dimension Tables

- **DimCustomers** (`dim_customers`): Customer identity and location (city, state, zip prefix).
- **DimProducts** (`dim_products`): Product attributes — category, weight, dimensions, photo count.
- **DimSellers** (`dim_sellers`): Seller identity and location.
- **DimDate** (`dim_date`): Calendar dimension (year, month, day, weekday, quarter) for time-series analysis.

| Table | Role | Row Count |
|---|---|---|
| `dim_customers` | Who the customer is | 99,441 |
| `dim_products` | What the product is | 32,951 |
| `dim_sellers` | Who the seller is | 3,095 |
| `dim_date` | When the order happened | varies (distinct purchase dates) |
| `fact_order_items` | The measurable event | **112,650** |

Each dimension relates to the fact table through a **One-to-Many** relationship, enforced by real **Foreign Key** constraints in PostgreSQL.

---

## Key Engineering Note: Surrogate Keys & Lazy Evaluation

One critical issue surfaced during development: Spark's `monotonically_increasing_id()` is **re-evaluated** (and can produce different values) every time a lazy DataFrame is recomputed — including during validation checks and the final write to PostgreSQL. This silently broke foreign key integrity between the fact table and its dimensions.

**Fix**: each dimension is `.cache()`-d immediately after its surrogate key is generated, freezing the values before any downstream use.

---

## Project Structure
.

├── etl_scripts/

│   ├── main.py         # Entry point — orchestrates Extract → Transform → Load

│   ├── extract.py      # Extract: reads raw CSV files

│   ├── transform.py    # Transform: builds dimensions, fact table, surrogate keys

│   └── load.py         # Load: truncates and writes to PostgreSQL

├── source_data/        # Raw CSV input files

├── data/                # Exported warehouse tables (post-load)

└── docs/

└── erd_diagram.png



---

## Data Source

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — real, anonymized commercial data, publicly licensed on Kaggle.