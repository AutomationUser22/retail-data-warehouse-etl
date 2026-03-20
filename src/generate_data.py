"""
Synthetic Superstore Sales Data Generator.

Generates a realistic retail dataset with customers, products, orders,
and financial metrics. Uses seeded random generation for reproducibility.
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Reference data for realistic generation ──

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SEGMENT_WEIGHTS = [0.52, 0.30, 0.18]

REGIONS = {
    "West": {"states": ["California", "Washington", "Oregon", "Colorado", "Arizona"],
             "cities": {"California": ["Los Angeles", "San Francisco", "San Diego"],
                        "Washington": ["Seattle", "Tacoma"],
                        "Oregon": ["Portland", "Eugene"],
                        "Colorado": ["Denver", "Boulder"],
                        "Arizona": ["Phoenix", "Tucson"]}},
    "East": {"states": ["New York", "Pennsylvania", "Massachusetts", "New Jersey", "Connecticut"],
             "cities": {"New York": ["New York City", "Buffalo", "Albany"],
                        "Pennsylvania": ["Philadelphia", "Pittsburgh"],
                        "Massachusetts": ["Boston", "Cambridge"],
                        "New Jersey": ["Newark", "Jersey City"],
                        "Connecticut": ["Hartford", "New Haven"]}},
    "Central": {"states": ["Texas", "Illinois", "Ohio", "Michigan", "Minnesota"],
                "cities": {"Texas": ["Houston", "Dallas", "Austin"],
                           "Illinois": ["Chicago", "Springfield"],
                           "Ohio": ["Columbus", "Cleveland"],
                           "Michigan": ["Detroit", "Ann Arbor"],
                           "Minnesota": ["Minneapolis", "Saint Paul"]}},
    "South": {"states": ["Florida", "Georgia", "North Carolina", "Virginia", "Tennessee"],
              "cities": {"Florida": ["Miami", "Orlando", "Tampa"],
                         "Georgia": ["Atlanta", "Savannah"],
                         "North Carolina": ["Charlotte", "Raleigh"],
                         "Virginia": ["Richmond", "Arlington"],
                         "Tennessee": ["Nashville", "Memphis"]}},
}

CATEGORIES = {
    "Furniture": {
        "sub_categories": ["Bookcases", "Chairs", "Tables", "Furnishings"],
        "price_range": (50, 2000),
        "manufacturers": ["Hon", "Bretford", "Safco", "Bush", "Ikea"],
    },
    "Office Supplies": {
        "sub_categories": ["Paper", "Binders", "Art", "Storage", "Envelopes", "Labels", "Fasteners", "Supplies"],
        "price_range": (2, 200),
        "manufacturers": ["Avery", "Acco", "Smead", "3M", "Staples"],
    },
    "Technology": {
        "sub_categories": ["Phones", "Accessories", "Machines", "Copiers"],
        "price_range": (20, 3000),
        "manufacturers": ["Apple", "Samsung", "Logitech", "Cisco", "Canon"],
    },
}

SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]
SHIP_MODE_DAYS = {"Standard Class": (5, 8), "Second Class": (3, 5), "First Class": (1, 3), "Same Day": (0, 0)}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Kevin", "Dorothy", "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
]


def generate_customers(num_customers: int, rng: random.Random) -> pd.DataFrame:
    """Generate customer dimension data."""
    customers = []
    used_ids = set()

    for _ in range(num_customers):
        # Generate unique customer ID
        while True:
            cid = f"CUS-{rng.randint(10000, 99999)}"
            if cid not in used_ids:
                used_ids.add(cid)
                break

        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        segment = rng.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        region = rng.choice(list(REGIONS.keys()))
        state = rng.choice(REGIONS[region]["states"])
        city = rng.choice(REGIONS[region]["cities"][state])
        postal = f"{rng.randint(10000, 99999)}"

        customers.append({
            "customer_id": cid,
            "customer_name": f"{first} {last}",
            "segment": segment,
            "region": region,
            "country": "United States",
            "state": state,
            "city": city,
            "postal_code": postal,
        })

    return pd.DataFrame(customers)


def generate_products(num_products: int, rng: random.Random) -> pd.DataFrame:
    """Generate product dimension data."""
    products = []
    used_ids = set()

    for _ in range(num_products):
        while True:
            pid = f"PRD-{rng.randint(10000, 99999)}"
            if pid not in used_ids:
                used_ids.add(pid)
                break

        category = rng.choice(list(CATEGORIES.keys()))
        cat_info = CATEGORIES[category]
        sub_cat = rng.choice(cat_info["sub_categories"])
        manufacturer = rng.choice(cat_info["manufacturers"])
        base_price = round(rng.uniform(*cat_info["price_range"]), 2)
        product_name = f"{manufacturer} {sub_cat[:-1] if sub_cat.endswith('s') else sub_cat} {rng.choice(['Pro', 'Elite', 'Basic', 'Standard', 'Plus', 'Max'])}"

        products.append({
            "product_id": pid,
            "product_name": product_name,
            "category": category,
            "sub_category": sub_cat,
            "manufacturer": manufacturer,
            "base_price": base_price,
        })

    return pd.DataFrame(products)


def generate_orders(
    num_orders: int,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    rng: random.Random,
    np_rng: np.random.Generator,
    date_start: str = "2020-01-01",
    date_end: str = "2024-12-31",
) -> pd.DataFrame:
    """Generate fact-level order/sales data."""
    start = datetime.strptime(date_start, "%Y-%m-%d")
    end = datetime.strptime(date_end, "%Y-%m-%d")
    date_range_days = (end - start).days

    orders = []
    order_counter = 0

    # Generate orders in batches (multi-item orders)
    while len(orders) < num_orders:
        order_counter += 1
        order_id = f"ORD-{order_counter:06d}"

        # Random order date with slight bias toward recent years
        day_offset = int(np_rng.beta(2, 1.5) * date_range_days)
        order_date = start + timedelta(days=day_offset)

        # Ship mode and ship date
        ship_mode = rng.choices(SHIP_MODES, weights=[0.60, 0.20, 0.15, 0.05], k=1)[0]
        ship_days = rng.randint(*SHIP_MODE_DAYS[ship_mode])
        ship_date = order_date + timedelta(days=ship_days)

        # Customer for this order
        customer = customers.iloc[rng.randint(0, len(customers) - 1)]

        # 1-5 items per order
        num_items = rng.choices([1, 2, 3, 4, 5], weights=[0.40, 0.30, 0.15, 0.10, 0.05], k=1)[0]
        selected_products = products.sample(n=min(num_items, len(products)), random_state=int(np_rng.integers(0, 100000)))

        for _, product in selected_products.iterrows():
            quantity = rng.choices(range(1, 15), weights=[30, 20, 15, 10, 8, 5, 4, 3, 2, 1, 1, 1, 0.5, 0.5], k=1)[0]

            # Discount logic: 30% of items get discounts
            if rng.random() < 0.30:
                discount = rng.choice([0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40])
            else:
                discount = 0.0

            unit_price = product["base_price"]
            sales_amount = round(unit_price * quantity * (1 - discount), 2)

            # Profit margin varies by category and discount
            base_margin = {"Furniture": 0.08, "Office Supplies": 0.25, "Technology": 0.15}
            margin = base_margin.get(product["category"], 0.15)
            if discount > 0.2:
                margin -= 0.15  # Heavy discounts eat into margin
            profit = round(sales_amount * (margin + rng.uniform(-0.05, 0.05)), 2)

            # Shipping cost
            shipping_cost = round(rng.uniform(2, 50) * (1 + quantity * 0.1), 2)

            orders.append({
                "order_id": order_id,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "ship_date": ship_date.strftime("%Y-%m-%d"),
                "ship_mode": ship_mode,
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "segment": customer["segment"],
                "region": customer["region"],
                "country": customer["country"],
                "state": customer["state"],
                "city": customer["city"],
                "postal_code": customer["postal_code"],
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "sub_category": product["sub_category"],
                "manufacturer": product["manufacturer"],
                "sales": sales_amount,
                "quantity": quantity,
                "discount": discount,
                "profit": profit,
                "shipping_cost": shipping_cost,
            })

            if len(orders) >= num_orders:
                break

    return pd.DataFrame(orders[:num_orders])


def generate_data(config_path: str = "config.yaml") -> str:
    """Generate synthetic retail dataset and save to CSV.

    Returns:
        Path to generated CSV file.
    """
    config = load_config(config_path)
    gen_config = config.get("data_generation", {})

    seed = gen_config.get("seed", 42)
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    num_customers = gen_config.get("num_customers", 800)
    num_products = gen_config.get("num_products", 1500)
    num_orders = gen_config.get("num_orders", 10000)

    print(f"Generating synthetic data (seed={seed})...")
    print(f"  Customers: {num_customers}")
    print(f"  Products:  {num_products}")
    print(f"  Orders:    {num_orders}")

    customers = generate_customers(num_customers, rng)
    products = generate_products(num_products, rng)

    date_range = config.get("transform", {}).get("date_range", {})
    orders = generate_orders(
        num_orders, customers, products, rng, np_rng,
        date_start=date_range.get("start", "2020-01-01"),
        date_end=date_range.get("end", "2024-12-31"),
    )

    # Save raw data
    raw_dir = config["paths"]["raw_data"]
    os.makedirs(raw_dir, exist_ok=True)
    output_path = os.path.join(raw_dir, config["extract"]["source_file"])
    orders.to_csv(output_path, index=False)

    print(f"  Generated {len(orders)} order lines → {output_path}")
    return output_path


if __name__ == "__main__":
    generate_data()
