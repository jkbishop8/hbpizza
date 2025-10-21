import pandas as pd
import random
import os
from datetime import datetime

# Load existing metrics
folder_path = os.path.join("C:\\","Users","fboy","source","repos", "HBPizza", "data_ingestion", "csv_data", "bronze")
file_path = os.path.join(folder_path, "store_metrics.csv")
df = pd.read_csv(file_path)
# Sample regions and store IDs
regions = ["Central", "Southeast", "Midwest", "Southwest", "Northeast"]
store_prefixes = ["TN", "KY", "IN", "GA", "OH"]

# Generate 100 new rows
new_rows = []
start_id = len(df) + 1

for i in range(100):
    store_id = f"{random.choice(store_prefixes)}{start_id + i:03}"
    region = random.choice(regions)
    total_sales = random.randint(5000, 15000)
    avg_delivery_time = random.randint(25, 60)
    ingestion_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Simulate missing metrics
    if random.random() < 0.08: total_sales = ""
    if random.random() < 0.05: avg_delivery_time = ""

    new_rows.append({
        "store_id": store_id,
        "region": region,
        "total_sales": total_sales,
        "avg_delivery_time": avg_delivery_time,
        "ingestion_timestamp": ingestion_timestamp
    })

# Append and save
new_df = pd.DataFrame(new_rows)
expanded_df = pd.concat([df, new_df], ignore_index=True)
expanded_df.to_csv(file_path, index=False)

