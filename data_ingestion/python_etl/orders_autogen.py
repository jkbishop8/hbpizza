import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Load existing orders
folder_path = os.path.join("C:\\","Users","fboy","source","repos", "HBPizza", "data_ingestion", "csv_data", "bronze")
file_path = os.path.join(folder_path, "orders.csv")
df = pd.read_csv(file_path)

# Sample values
store_ids = ["TN001", "TN002", "TN003", "TN004", "TN005", "KY001", "IN003"]
pizza_sizes = ["Small", "Medium", "Large"]
topping_pool = ["Pepperoni", "Mushroom", "Onion", "Sausage", "Bacon", "Green Peppers", "Olives", "Pineapple", "Cheese"]

# Generate 100 new rows
new_rows = []
start_id = df["order_id"].max() + 1 if not df.empty else 1000

for i in range(100):
    order_id = start_id + i
    timestamp = (datetime.now() - timedelta(minutes=random.randint(0, 10000))).strftime("%m/%d/%Y %H:%M:%S")
    store_id = random.choice(store_ids)
    pizza_size = random.choice(pizza_sizes)
    toppings = ";".join(random.sample(topping_pool, random.randint(1, 3)))
    total_price = round(random.uniform(10.0, 22.0), 2)

    # Simulate missing data
    if random.random() < 0.05: timestamp = ""
    if random.random() < 0.07: store_id = ""
    if random.random() < 0.1: toppings = ""

    new_rows.append({
        "order_id": order_id,
        "timestamp": timestamp,
        "store_id": store_id,
        "pizza_size": pizza_size,
        "toppings": toppings,
        "total_price": total_price
    })

# Append and save
new_df = pd.DataFrame(new_rows)
expanded_df = pd.concat([df, new_df], ignore_index=True)
expanded_df.to_csv(file_path, index=False)
