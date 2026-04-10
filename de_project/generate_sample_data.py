"""
generate_sample_data.py – Generates a sample sales transaction CSV for Part 2 testing.

Run: python generate_sample_data.py
Writes: input/sales_transactions.csv  (clean)
        input/sales_transactions_dirty.csv  (with intentional errors for testing)
"""

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent / "input"
OUTPUT_DIR.mkdir(exist_ok=True)

CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports", "Toys", "Beauty"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "PayPal", "Bank Transfer", "Voucher"]
PRODUCTS = {
    "Electronics": [("Laptop", "P001"), ("Smartphone", "P002"), ("Headphones", "P003"), ("Tablet", "P004")],
    "Clothing":    [("T-Shirt", "P010"), ("Jeans", "P011"), ("Jacket", "P012"), ("Shoes", "P013")],
    "Food":        [("Coffee Beans", "P020"), ("Chocolate Box", "P021"), ("Tea Pack", "P022")],
    "Books":       [("Python Cookbook", "P030"), ("Data Engineering", "P031"), ("Clean Code", "P032")],
    "Home":        [("Lamp", "P040"), ("Cushion Set", "P041"), ("Candle Pack", "P042")],
    "Sports":      [("Yoga Mat", "P050"), ("Dumbbell Set", "P051"), ("Running Shoes", "P052")],
    "Toys":        [("Lego Set", "P060"), ("Board Game", "P061"), ("Puzzle", "P062")],
    "Beauty":      [("Face Cream", "P070"), ("Shampoo", "P071"), ("Perfume", "P072")],
}
REGIONS = ["Brussels", "Ghent", "Antwerp", "Bruges", "Leuven"]
STORES = ["S01", "S02", "S03", "S04", "S05"]


def _random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_clean(n: int = 200) -> pd.DataFrame:
    rows = []
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 31)

    seen_tx_ids = set()
    for _ in range(n):
        tx_id = str(uuid.uuid4())[:8].upper()
        while tx_id in seen_tx_ids:
            tx_id = str(uuid.uuid4())[:8].upper()
        seen_tx_ids.add(tx_id)

        category = random.choice(CATEGORIES)
        product_name, product_id = random.choice(PRODUCTS[category])
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(1.0, 500.0), 2)
        discount = round(random.uniform(0, 30), 1) if random.random() > 0.5 else 0.0
        total = round(quantity * unit_price * (1 - discount / 100), 2)

        rows.append({
            "transaction_id": tx_id,
            "timestamp": _random_date(start, end).strftime("%Y-%m-%d %H:%M:%S"),
            "customer_id": f"C{random.randint(1000, 9999)}",
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "discount_pct": discount,
            "store_id": random.choice(STORES),
            "region": random.choice(REGIONS),
            "return_flag": random.choice([True, False]),
            "notes": random.choice(["", "Gift wrap requested", "Express delivery", ""]),
        })

    return pd.DataFrame(rows)


def generate_dirty(clean_df: pd.DataFrame) -> pd.DataFrame:
    dirty = clean_df.copy()

    # Inject ~10% errors of various types
    n = len(dirty)
    idx = dirty.index.tolist()

    # Duplicate 5 rows
    dupes = dirty.sample(5)
    dirty = pd.concat([dirty, dupes], ignore_index=True)

    # Nulls in mandatory column
    for i in random.sample(idx, 5):
        dirty.at[i, "customer_id"] = None

    # Invalid category
    for i in random.sample(idx, 3):
        dirty.at[i, "category"] = "Unknown"

    # Negative unit price
    for i in random.sample(idx, 3):
        dirty.at[i, "unit_price"] = -abs(dirty.at[i, "unit_price"])

    # Invalid payment method
    for i in random.sample(idx, 3):
        dirty.at[i, "payment_method"] = "Crypto"

    # Quantity = 0
    for i in random.sample(idx, 2):
        dirty.at[i, "quantity"] = 0

    return dirty


if __name__ == "__main__":
    clean = generate_clean(200)
    clean.to_csv(OUTPUT_DIR / "sales_transactions.csv", index=False)
    print(f"Clean dataset: {OUTPUT_DIR / 'sales_transactions.csv'}  ({len(clean)} rows)")

    dirty = generate_dirty(clean)
    dirty.to_csv(OUTPUT_DIR / "sales_transactions_dirty.csv", index=False)
    print(f"Dirty dataset: {OUTPUT_DIR / 'sales_transactions_dirty.csv'}  ({len(dirty)} rows)")