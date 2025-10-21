import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Load existing inventory
folder_path = os.path.join("C:\\","Users","fboy","source","repos", "HBPizza", "data_ingestion", "csv_data", "bronze")
file_path = os.path.join(folder_path, "inventory.csv")
df = pd.read_csv(file_path)


# Sample ingredients and suppliers
ingredients = ["Bacon", "Green Peppers", "Onions", "Sausage", "Olives", "Pineapple", "Chicken"]
suppliers = ["MeatMakers", "VeggieVault", "FreshFarm", "ToppingTown", "FlavorFlow"]

# Generate 100 new rows
new_rows = []
for i in range(100):
    ingredient = random.choice(ingredients)
    supplier = random.choice(suppliers)
    cost = round(random.uniform(0.4, 1.2), 2)
    stock = random.randint(100, 200)
    last_updated = (datetime.today() - timedelta(days=random.randint(0, 10))).strftime("%Y-%m-%d")
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    new_rows.append({
        "ingredient": ingredient,
        "supplier": supplier,
        "cost_per_unit": cost,
        "stock_level": stock,
        "last_updated": last_updated,
        "ingestion_timestamp": ingestion_timestamp
    })

# Append and save


new_df = pd.DataFrame(new_rows)
expanded_df = pd.concat([df, new_df], ignore_index=True)
# Save to new CSV
expanded_df.to_csv(file_path, index=False)
