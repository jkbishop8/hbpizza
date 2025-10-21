import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Load existing data
# Load existing inventory
folder_path = os.path.join("C:\\","Users","fboy","source","repos", "HBPizza", "data_ingestion", "csv_data", "bronze")
file_path = os.path.join(folder_path, "locations.csv")
df = pd.read_csv(file_path)
# Sample values
store_types = ["Gas Station", "Convenience Store", "Local Grocer", "Supermarket", "Food Court"]
cities = ["Murfreesboro", "Smyrna", "La Vergne", "Lebanon", "Nashville"]
states = ["TN"]
wing_options = ["Yes", "No"]

# Generate 100 new rows with occasional missing fields
new_rows = []
start_id = len(df) + 1

for i in range(100):
    store_id = f"TN{start_id + i:03}"
    store_name = f"Store {start_id + i}"
    store_type = random.choice(store_types)
    address = f"{random.randint(100, 999)} {random.choice(['Main St', 'Broadway', 'Elm St', 'Maple Ave'])}"
    city = random.choice(cities)
    state = random.choice(states)
    zip_code = f"{random.randint(37000, 37999)}"
    offers_wings = random.choice(wing_options) 
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Randomly omit some fields to simulate data collection issues
    if random.random() < 0.1: address = ""
    if random.random() < 0.1: zip_code = ""
    if random.random() < 0.05: offers_wings = ""

    new_rows.append({
        "store_id": store_id,
        "store_name": store_name,
        "store_type": store_type,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "offers_wings": offers_wings,
        "ingestion_timestamp": ingestion_timestamp
    })

# Append and save
new_df = pd.DataFrame(new_rows)
expanded_df = pd.concat([df, new_df], ignore_index=True)
expanded_df.to_csv(file_path, index=False)
