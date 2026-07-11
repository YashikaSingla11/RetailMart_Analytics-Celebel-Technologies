# Databricks notebook source
import pandas as pd
import numpy as np
import os

current_dir = os.getcwd()
raw_dir = os.path.join(current_dir, "data")

print("--- Initializing Pandas Data Cleaning Layer ---")
# Pandas se raw local CSV read kar rahe hain
df_cust = pd.read_csv(os.path.join(raw_dir, "customers.csv"))
df_prod = pd.read_csv(os.path.join(raw_dir, "products.csv"))
df_ord = pd.read_csv(os.path.join(raw_dir, "orders.csv"))

# COMMAND ----------

# PANDAS TRANSFORMATION: Dropping rows where tracking keys are null
print(f"Total raw orders rows: {len(df_ord)}")
df_ord_cleaned = df_ord.dropna(subset=["customer_id"]).copy()
print(f"Orders rows after removing null customer IDs: {len(df_ord_cleaned)}")

# Filling remaining descriptive blanks with text standard placeholders
df_cust = df_cust.fillna("Unknown")
df_prod = df_prod.fillna("Unknown")

# COMMAND ----------

# PANDAS TRANSFORMATION: Calculating delivery_days business metric
df_ord_cleaned["order_date"] = pd.to_datetime(df_ord_cleaned["order_date"])
df_ord_cleaned["delivery_date"] = pd.to_datetime(df_ord_cleaned["delivery_date"])

# Delivery days delta calculation
df_ord_cleaned["delivery_days"] = (df_ord_cleaned["delivery_date"] - df_ord_cleaned["order_date"]).dt.days

# Filling null fields (for non-delivered orders) with default flag -1
df_ord_cleaned["delivery_days"] = df_ord_cleaned["delivery_days"].fillna(-1).astype(int)

# Re-converting timestamps back to string format for reliable lakehouse loading
df_ord_cleaned["order_date"] = df_ord_cleaned["order_date"].dt.strftime("%Y-%m-%d")
df_ord_cleaned["delivery_date"] = df_ord_cleaned["delivery_date"].fillna("").astype(str)

# COMMAND ----------

# Saving cleaned inputs inside data/cleaned folder structures
cleaned_dir = os.path.join(raw_dir, "cleaned")
os.makedirs(cleaned_dir, exist_ok=True)

df_cust.to_csv(os.path.join(cleaned_dir, "customers_clean.csv"), index=False)
df_prod.to_csv(os.path.join(cleaned_dir, "products_clean.csv"), index=False)
df_ord_cleaned.to_csv(os.path.join(cleaned_dir, "orders_clean.csv"), index=False)

print(f"🏆 Cleaned datasets ready and dumped at: {cleaned_dir}")

# COMMAND ----------

