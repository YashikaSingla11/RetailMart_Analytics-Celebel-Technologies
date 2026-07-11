# Databricks notebook source
# MAGIC %sql
# MAGIC -- Setting database target workspace explicitly
# MAGIC USE CATALOG retailmart_catalog;
# MAGIC USE SCHEMA operational_lakehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REQUIREMENT 1: SQL SELECT/Keys & WHERE (Filter by status values)
# MAGIC SELECT order_id, customer_sk, order_status, total_sales_amount 
# MAGIC FROM fact_orders 
# MAGIC WHERE order_status = 'Delivered';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REQUIREMENT 2: GROUP BY + JOINS (Customer 360 & Revenue metrics evaluation)
# MAGIC SELECT 
# MAGIC     c.customer_name, 
# MAGIC     c.email,
# MAGIC     COUNT(f.order_id) as total_orders_placed,
# MAGIC     SUM(f.total_sales_amount) as total_revenue_yield
# MAGIC FROM dim_customer c
# MAGIC JOIN fact_orders f ON c.customer_sk = f.customer_sk
# MAGIC GROUP BY c.customer_name, c.email
# MAGIC ORDER BY total_revenue_yield DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REQUIREMENT 3: Conditional CASE Statements execution (Loyalty Segments Profiling)
# MAGIC SELECT 
# MAGIC     customer_sk, 
# MAGIC     COUNT(order_id) as total_transactions,
# MAGIC     CASE 
# MAGIC         WHEN COUNT(order_id) >= 5 THEN 'Elite Loyalty Tier'
# MAGIC         WHEN COUNT(order_id) BETWEEN 2 AND 4 THEN 'Regular Core Tier'
# MAGIC         ELSE 'Standard Entry Tier' 
# MAGIC     END AS customer_loyalty_segment
# MAGIC FROM fact_orders 
# MAGIC GROUP BY customer_sk;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REQUIREMENT 4: Subqueries Isolation (Isolating Above-Average High Spending Orders)
# MAGIC SELECT order_id, customer_sk, total_sales_amount 
# MAGIC FROM fact_orders
# MAGIC WHERE total_sales_amount > (SELECT AVG(total_sales_amount) FROM fact_orders)
# MAGIC ORDER BY total_sales_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REQUIREMENT 5: CTEs & Window Functions combined (Rank Trending Products within Categories)
# MAGIC WITH TopTrendingProductsCTE AS (
# MAGIC     SELECT 
# MAGIC         p.product_name, 
# MAGIC         p.category, 
# MAGIC         SUM(f.quantity) as total_units_sold,
# MAGIC         DENSE_RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.quantity) DESC) as sales_performance_rank
# MAGIC     FROM dim_product p
# MAGIC     JOIN fact_orders f ON p.product_sk = f.product_sk
# MAGIC     GROUP BY p.product_name, p.category
# MAGIC )
# MAGIC SELECT * FROM TopTrendingProductsCTE 
# MAGIC WHERE sales_performance_rank = 1;

# COMMAND ----------

import os

# Check raw data folder
print("--- Raw Files ---")
print("customers.csv exists:", os.path.exists("data/customers.csv"))
print("products.csv exists:", os.path.exists("data/products.csv"))
print("orders.csv exists:", os.path.exists("data/orders.csv"))

# Check pandas cleaned data folder
print("\n--- Cleaned Files ---")
print("customers_clean.csv exists:", os.path.exists("data/cleaned/customers_clean.csv"))
print("products_clean.csv exists:", os.path.exists("data/cleaned/products_clean.csv"))
print("orders_clean.csv exists:", os.path.exists("data/cleaned/orders_clean.csv"))