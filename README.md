# RetailMart Data Engineering Lakehouse Pipeline

An end-to-end cloud data engineering production pipeline designed inside Databricks using the **Medallion Architecture**. The project processes messy operational e-commerce records, standardizes dimensional schemas, handles temporal variations, and builds a robust Star Schema optimized for advanced business insights using PySpark and Databricks SQL.

---

## Tech Stack & Key Concepts Handled
*   **Language & Core Processing:** Python, Pandas, PySpark DataFrame API
*   **Storage & Framework:** Delta Lake Format, Databricks Unity Catalog
*   **Architecture Design:** Medallion Framework (Bronze to  Silver to  Gold), Star Schema Modeling (Fact & Dimension Tables)
*   **Analytical Engine:** Advanced Databricks SQL (CTEs, Subqueries, Window Functions, Aggregate Queries)
*   **Data Strategy:** Schema Enforcement, Type Standardization, Surrogate Key Generation (`row_number()`), and SCD Type 2 Tracking Strategy

---

## Repository Structure
```text
├── data/                         # Local workspace directory for data assets
│   ├── customers.csv             # Raw Customer demographic log inputs
│   ├── products.csv              # Raw Product retail catalog items
│   ├── orders.csv                # Raw, messy Transaction order histories
│   └── cleaned/                  # Intermediate storage for pre-processed logs
│       ├── customers_clean.csv
│       ├── products_clean.csv
│       └── orders_clean.csv
├── 1_data_generation.ipynb       # Notebook 1: Mock data generation engine
├── 2_pandas_cleaning.ipynb       # Notebook 2: Initial cleansing & metric formulation
├── 3_medallion_pipeline.ipynb    # Notebook 3: Multi-stage Medallion ETL framework
└── 4_business_insights.sql       # Notebook 4: Pure SQL Advanced Analytics Scripts
