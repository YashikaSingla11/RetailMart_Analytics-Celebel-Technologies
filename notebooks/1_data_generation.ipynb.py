# Databricks notebook source
#  Faker library install 
# Dependencies install 
%pip install faker pandas openpyxl

# COMMAND ----------

import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(101)
random.seed(101)

# Current absolute path automatic fetch karne ke liye taaki path galat na ho
current_dir = os.getcwd()
output_dir = os.path.join(current_dir, "data")
os.makedirs(output_dir, exist_ok=True)

print(f"📁 Target physical data directory setup at: {output_dir}")

# COMMAND ----------

# 1. GENERATING CUSTOMERS RAW DATASET
cust_ids = [f"CUST_{i:04d}" for i in range(1, 101)]
customers_df = pd.DataFrame({
    "customer_id": cust_ids,
    "customer_name": [fake.name() for _ in range(100)],
    "email": [fake.unique.email() for _ in range(100)],
    "country": [fake.country() for _ in range(100)]
})

customers_path = os.path.join(output_dir, "customers.csv")
customers_df.to_csv(customers_path, index=False)
print(f"✅ Created: {customers_path}")

# COMMAND ----------

# 2. GENERATING PRODUCTS RAW CATALOG
product_items = [
    {"product_name": "MacBook Air", "category": "Electronics", "price": 95000},
    {"product_name": "iPhone 15 Pro", "category": "Electronics", "price": 120000},
    {"product_name": "Wireless Headphones", "category": "Audio", "price": 15000},
    {"product_name": "Smart Fitness Band", "category": "Wearables", "price": 8000},
    {"product_name": "Gaming Keyboard", "category": "Accessories", "price": 6000},
    {"product_name": "Bluetooth Speaker", "category": "Audio", "price": 5000},
    {"product_name": "4K Ultra Monitor", "category": "Electronics", "price": 32000},
    {"product_name": "Type-C Charging Hub", "category": "Accessories", "price": 2500}
]

prod_ids = [f"PROD_{i:02d}" for i in range(1, len(product_items) + 1)]
for idx, item in enumerate(product_items):
    item["product_id"] = prod_ids[idx]

products_df = pd.DataFrame(product_items)
products_path = os.path.join(output_dir, "products.csv")
products_df.to_csv(products_path, index=False)
print(f"✅ Created: {products_path}")

# COMMAND ----------

# 3. GENERATING ORDERS TRANSACTION LOGS (With Nulls and dates for transformation check)
orders_data = []
order_statuses = ["Delivered", "Shipped", "Pending", "Cancelled"]
base_date = datetime(2026, 1, 1)

for i in range(1, 401):
    o_date = base_date + timedelta(days=random.randint(0, 100))
    status = random.choices(order_statuses, weights=[0.65, 0.15, 0.10, 0.10], k=1)[0]
    del_date = (o_date + timedelta(days=random.randint(1, 6))).strftime("%Y-%m-%d") if status == "Delivered" else None
    
    orders_data.append({
        "order_id": f"ORD_{i:05d}",
        "customer_id": random.choice(cust_ids) if random.random() > 0.02 else None, # Intentionally adding 2% null values for cleaning requirement
        "product_id": random.choice(prod_ids),
        "order_date": o_date.strftime("%Y-%m-%d"),
        "delivery_date": del_date,
        "status": status,
        "quantity": random.choices([1, 2, 3], weights=[0.75, 0.20, 0.05], k=1)[0]
    })

orders_df = pd.DataFrame(orders_data)
orders_path = os.path.join(output_dir, "orders.csv")
orders_df.to_csv(orders_path, index=False)
print(f"✅ Created: {orders_path}")

# COMMAND ----------

