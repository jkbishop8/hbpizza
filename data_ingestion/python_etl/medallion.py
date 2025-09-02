# python_etl/medallion_etl.py
import pandas as pd
from pathlib import Path

# Optional: pip install textblob && python -m textblob.download_corpora
try:
    from textblob import TextBlob
except Exception:
    TextBlob = None

BASE = Path(__file__).resolve().parents[1]  # repo root
BRONZE = BASE / "data_ingestion" / "bronze"
SILVER = BASE / "data_ingestion" / "silver"
GOLD = BASE / "data_ingestion" / "gold"
SILVER.mkdir(parents=True, exist_ok=True)
GOLD.mkdir(parents=True, exist_ok=True)

def normalize_combo(toppings):
    if pd.isna(toppings) or toppings == "":
        return "CheeseOnly"
    items = [t.strip().title() for t in str(toppings).split(";") if t.strip()]
    return "+".join(sorted(items)) if items else "CheeseOnly"

# --- Bronze -> Silver ---
orders = pd.read_csv(BRONZE / "orders.csv")
orders["timestamp"] = pd.to_datetime(orders["timestamp"], errors="coerce")
orders["total_price"] = pd.to_numeric(orders["total_price"], errors="coerce")
orders["pizza_size"] = orders["pizza_size"].str.title()
orders["pizza_combo"] = orders["toppings"].apply(normalize_combo)

locations = pd.read_csv(BRONZE / "hunt_locations.csv")
locations["offers_wings"] = locations["offers_wings"].str.strip().str.lower().isin(["yes", "true", "1"])
locations["store_type"] = locations["store_type"].str.title()

metrics = pd.read_csv(BRONZE / "store_metrics.csv")
metrics["region"] = metrics["region"].str.title()

feedback = pd.read_csv(BRONZE / "feedback.csv")
feedback["rating"] = pd.to_numeric(feedback["rating"], errors="coerce")
if "comment" in feedback:
    feedback["comment"] = feedback["comment"].fillna("").astype(str)
if TextBlob:
    feedback["sentiment_polarity"] = feedback["comment"].apply(lambda t: TextBlob(t).sentiment.polarity if t else 0.0)
    feedback["sentiment_label"] = pd.cut(
        feedback["sentiment_polarity"], bins=[-1.01, -0.2, 0.2, 1.01], labels=["Negative", "Neutral", "Positive"]
    )
else:
    feedback["sentiment_polarity"] = 0.0
    feedback["sentiment_label"] = "Neutral"

# Save Silver
orders.to_csv(SILVER / "orders_silver.csv", index=False)
locations.to_csv(SILVER / "locations_silver.csv", index=False)
metrics.to_csv(SILVER / "store_metrics_silver.csv", index=False)
feedback.to_csv(SILVER / "feedback_silver.csv", index=False)

# --- Silver -> Gold (star schema) ---
# Dim Date
dim_date = (
    orders[["timestamp"]].dropna().assign(
        date=lambda df: df["timestamp"].dt.date,
        year=lambda df: df["timestamp"].dt.year,
        quarter=lambda df: df["timestamp"].dt.quarter,
        month=lambda df: df["timestamp"].dt.month,
        day=lambda df: df["timestamp"].dt.day
    )[["date", "year", "quarter", "month", "day"]].drop_duplicates().reset_index(drop=True)
)
dim_date["date_key"] = pd.factorize(dim_date["date"])[0] + 1

# Dim Store (join locations + metrics)
dim_store = locations.merge(metrics, on="store_id", how="left")
dim_store["store_key"] = pd.factorize(dim_store["store_id"])[0] + 1

# Dim Product (size + combo)
dim_product = orders[["pizza_size", "pizza_combo"]].drop_duplicates().reset_index(drop=True)
dim_product["product_key"] = pd.factorize(dim_product["pizza_size"] + "|" + dim_product["pizza_combo"])[0] + 1

# Fact Orders
orders_fact = orders.merge(dim_store[["store_id", "store_key"]], on="store_id", how="left") \
    .merge(dim_product, on=["pizza_size", "pizza_combo"], how="left") \
    .merge(dim_date[["date", "date_key"]], left_on=orders["timestamp"].dt.date, right_on="date", how="left")

orders_fact = orders_fact.rename(columns={"order_id": "order_key"})[
    ["order_key", "store_key", "product_key", "date_key", "total_price"]
]

# Fact Feedback
feedback_fact = feedback.merge(orders[["order_id", "store_id", "timestamp"]], on="order_id", how="left") \
    .merge(dim_store[["store_id", "store_key"]], on="store_id", how="left") \
    .merge(dim_date[["date", "date_key"]], left_on=feedback["timestamp"].dt.date if "timestamp" in feedback else orders["timestamp"].dt.date, right_on="date", how="left")

feedback_fact = feedback_fact.rename(columns={"feedback_id": "feedback_key"})[
    ["feedback_key", "store_key", "date_key", "rating", "sentiment_polarity"]
]

# Save Gold
for name, df in [
    ("dim_date.csv", dim_date[["date_key", "date", "year", "quarter", "month", "day"]]),
    ("dim_store.csv", dim_store[["store_key", "store_id", "store_name", "store_type", "city", "state", "zip", "region", "offers_wings", "total_sales", "avg_delivery_time"]]
        .rename(columns={"total_sales":"store_total_sales"})),
    ("dim_product.csv", dim_product[["product_key", "pizza_size", "pizza_combo"]]),
    ("fact_orders.csv", orders_fact),
    ("fact_feedback.csv", feedback_fact),
]:
    (GOLD / name).write_text("") if df.empty else df.to_csv(GOLD / name, index=False)

print("Silver and Gold layers generated.")
