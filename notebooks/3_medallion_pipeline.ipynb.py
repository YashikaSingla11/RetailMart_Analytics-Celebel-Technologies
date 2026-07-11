# Databricks notebook source
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Setup Databricks Managed Catalog Schema locations
spark.sql("CREATE CATALOG IF NOT EXISTS retailmart_catalog")
spark.sql("USE CATALOG retailmart_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS operational_lakehouse")
spark.sql("USE SCHEMA operational_lakehouse")

current_dir = os.getcwd()
cleaned_dir_path = os.path.join(current_dir, "data", "cleaned")
print(f"--- Medallion Framework Rooted at: file:{cleaned_dir_path} ---")

# COMMAND ----------

# -------------------------------------------------------------
# A. BRONZE LAYER COMPONENT SETUP (Strict Ingestion Preservation)
# -------------------------------------------------------------
print("Starting Bronze structural pipeline mapping...")

def ingest_to_bronze_table(local_filename, target_table_name):
    full_spark_local_path = f"file:{os.path.join(cleaned_dir_path, local_filename)}"
    spark_df = spark.read.csv(full_spark_local_path, header=True, inferSchema=False)
    
    # Cast every item to string for schema safety rules
    for attribute in spark_df.columns:
        spark_df = spark_df.withColumn(attribute, F.col(attribute).cast("string"))
        
    bronze_final = spark_df.withColumn("lakehouse_ingestion_at", F.current_timestamp())
    bronze_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"bronze_{target_table_name}")
    print(f"✅ Success: Table 'bronze_{target_table_name}' saved.")

ingest_to_bronze_table("customers_clean.csv", "customers")
ingest_to_bronze_table("products_clean.csv", "products")
ingest_to_bronze_table("orders_clean.csv", "orders")

# COMMAND ----------

# -------------------------------------------------------------
# B. SILVER LAYER COMPONENT SETUP (Surrogate Keys & Types Mapping)
# -------------------------------------------------------------
print("Transforming Bronze inputs to optimized Silver tables...")

# 1. Processing Dimension Table Products (With product_sk Surrogate Key)
df_b_prod = spark.table("bronze_products")
df_s_prod_base = df_b_prod.withColumn("price", F.col("price").cast("double"))

window_prod = Window.orderBy("product_id")
df_silver_prod = df_s_prod_base.withColumn("product_sk", F.row_number().over(window_prod))
df_silver_prod.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_products")

# 2. Processing Dimension Table Customers (With customer_sk Surrogate Key)
df_b_cust = spark.table("bronze_customers")
df_s_cust_base = df_b_cust.withColumn("is_current", F.lit(True)) \
                          .withColumn("effective_start_date", F.current_date()) \
                          .withColumn("effective_end_date", F.lit(None).cast("date"))

window_cust = Window.orderBy("customer_id")
df_silver_cust = df_s_cust_base.withColumn("customer_sk", F.row_number().over(window_cust))
df_silver_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_customers")

# COMMAND ----------

# 3. Processing Core Orders Fact Records (With order_sk Surrogate Key)
df_b_ord = spark.table("bronze_orders")
df_s_ord_base = df_b_ord.withColumn("quantity", F.col("quantity").cast("int")) \
                        .withColumn("delivery_days", F.col("delivery_days").cast("int"))

window_orders = Window.orderBy("order_id")
df_silver_orders = df_s_ord_base.withColumn("order_sk", F.row_number().over(window_orders))
df_silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")

print("🏆 Silver Layer datasets processed smoothly with Surrogate Keys!")

# COMMAND ----------

# -------------------------------------------------------------
# C. GOLD LAYER GENERATION (Star Schema Warehouse Compilation)
# -------------------------------------------------------------
print("Compiling Gold Data Warehouse Tables...")

df_prod_dim = spark.table("silver_products")
df_cust_dim = spark.table("silver_customers")
df_ord_fact = spark.table("silver_orders")

# Creating decoupled Gold Dimension tables
df_prod_dim.select("product_sk", "product_id", "product_name", "category", "price").write.format("delta").mode("overwrite").saveAsTable("dim_product")
df_cust_dim.select("customer_sk", "customer_id", "customer_name", "email", "country", "is_current").write.format("delta").mode("overwrite").saveAsTable("dim_customer")

# Generating Central Gold Sales Fact Table with correct schema joining
fact_orders_compiled = df_ord_fact \
    .join(df_prod_dim, "product_id", "left") \
    .join(df_cust_dim, "customer_id", "left") \
    .select(
        df_ord_fact["order_sk"],
        df_cust_dim["customer_sk"],
        df_prod_dim["product_sk"],
        df_ord_fact["order_id"],
        df_ord_fact["order_date"],
        df_ord_fact["status"].alias("order_status"),
        df_ord_fact["quantity"],
        df_ord_fact["delivery_days"],
        (df_ord_fact["quantity"] * df_prod_dim["price"]).alias("total_sales_amount")
    )

fact_orders_compiled.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_orders")
print("⭐ Gold Star Schema complete! Ready for SQL Analytics.")