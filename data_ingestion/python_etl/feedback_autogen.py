import pandas as pd
import random
import os

# Load your existing dataset
folder_path = os.path.join("C:\\","Users","fboy","source","repos", "HBPizza", "data_ingestion", "csv_data", "bronze")
file_path = os.path.join(folder_path, "feedback.csv")
df = pd.read_csv(file_path)

# Define sample comments by category and sentiment
feedback_templates = {
    "Taste": {
        "Positive": [
            "Pizza was perfectly seasoned!",
            "Loved the spicy wings!",
            "Crust was crispy and delicious.",
            "Toppings were fresh and generous."
        ],
        "Neutral": [
            "Pizza was okay, nothing special.",
            "Wings were average.",
            "Flavor was mild.",
            "Not bad, but not memorable."
        ],
        "Negative": [
            "Pizza was bland and soggy.",
            "Wings were burnt.",
            "Crust tasted undercooked.",
            "Too greasy and salty."
        ]
    },
    "Delivery": {
        "Positive": [
            "Fast and friendly delivery!",
            "Driver was polite and on time.",
            "Order arrived hot and fresh.",
            "Great delivery service."
        ],
        "Neutral": [
            "Delivery was fine.",
            "Arrived on time, no issues.",
            "Standard delivery experience.",
            "Nothing to complain about."
        ],
        "Negative": [
            "Delivery was late.",
            "Driver got lost.",
            "Food arrived cold.",
            "Poor delivery timing."
        ]
    },
    "Service": {
        "Positive": [
            "Staff was helpful and courteous.",
            "Great customer service!",
            "Easy ordering process.",
            "Very responsive team."
        ],
        "Neutral": [
            "Service was acceptable.",
            "No issues with staff.",
            "Ordering was fine.",
            "Average experience."
        ],
        "Negative": [
            "Rude staff.",
            "Order was incorrect.",
            "Long wait time.",
            "Poor customer support."
        ]
    },
    "Portion": {
        "Positive": [
            "Generous portion size!",
            "Plenty of wings for the price.",
            "Pizza was big and filling.",
            "Great value for money."
        ],
        "Neutral": [
            "Portion was decent.",
            "Enough for one person.",
            "Not too much, not too little.",
            "Average portion."
        ],
        "Negative": [
            "Tiny wings.",
            "Pizza was too small.",
            "Not enough food.",
            "Skimpy toppings."
        ]
    }
}

# Generate 100 new rows
new_rows = []
start_id = len(df) + 1
for i in range(100):
    category = random.choice(list(feedback_templates.keys()))
    sentiment = random.choice(list(feedback_templates[category].keys()))
    comment = random.choice(feedback_templates[category][sentiment])
    rating = {
        "Positive": random.randint(4, 5),
        "Neutral": 3,
        "Negative": random.randint(1, 2)
    }[sentiment]
    
    new_rows.append({
        "feedback_id": f"F{start_id + i:03}",
        "order_id": 1000 + start_id + i,
        "comment": comment,
        "rating": rating,
        "category": category,
        "sentiment": sentiment
    })

# Convert to DataFrame and append
new_df = pd.DataFrame(new_rows)
expanded_df = pd.concat([df, new_df], ignore_index=True)

# Save to new CSV
expanded_df.to_csv(file_path, index=False)
