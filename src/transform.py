"""
Transform Module — Build star schema from raw extracted data.

Responsibilities:
- Generate surrogate keys for all dimensions
- Build dim_customer, dim_product, dim_date, dim_ship_mode
- Construct fact_sales with foreign key references
- Derive calculated fields and business logic
- SCD Type 1 handling (latest value wins)
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def build_dim_date(config_path: str = "config.yaml") -> pd.DataFrame:
    """Build date dimension table covering the full date range.

    Includes calendar attributes, fiscal quarter mapping, and
    convenience flags for analytical queries.
    """
    config = load_config(config_path)
    transform_cfg = config.get("transform", {})
    date_range = transform_cfg.get("date_range", {})
    fiscal_start = transform_cfg.get("fiscal_year_start_month", 4)

    start = pd.Timestamp(date_range.get("start", "2020-01-01"))
    end = pd.Timestamp(date_range.get("end", "2024-12-31"))

    dates = pd.date_range(start=start, end=end, freq="D")

    dim_date = pd.DataFrame({
        "date_key": range(1, len(dates) + 1),
        "full_date": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "day_of_month": dates.day,
        "day_of_week": dates.dayofweek,  # 0=Monday
        "day_name": dates.strftime("%A"),
        "week_of_year": dates.isocalendar().week.astype(int),
        "is_weekend": dates.dayofweek.isin([5, 6]).astype(int),
    })

    # Fiscal quarter: if fiscal year starts in April, then Apr=Q1, Jul=Q2, Oct=Q3, Jan=Q4
    def fiscal_quarter(month):
        adjusted = (month - fiscal_start) % 12
        return adjusted // 3 + 1

    dim_date["fiscal_quarter"] = dim_date["month"].apply(fiscal_quarter)
    dim_date["fiscal_year"] = dim_date.apply(
        lambda r: r["year"] if r["month"] >= fiscal_start else r["year"] - 1, axis=1
    )

    return dim_date


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """Build customer dimension with SCD Type 1 (latest value wins)."""
    # Take the most recent record per customer (last order = latest attributes)
    customer_cols = [
        "customer_id", "customer_name", "segment",
        "region", "country", "state", "city", "postal_code",
    ]

    # Sort by order_date descending, take first per customer_id
    dim_customer = (
        df.sort_values("order_date", ascending=False)
        .drop_duplicates(subset=["customer_id"], keep="first")[customer_cols]
        .reset_index(drop=True)
    )

    dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))
    return dim_customer


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Build product dimension from raw data."""
    product_cols = [
        "product_id", "product_name", "category", "sub_category", "manufacturer",
    ]

    dim_product = (
        df.drop_duplicates(subset=["product_id"], keep="first")[product_cols]
        .reset_index(drop=True)
    )

    dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))
    return dim_product


def build_dim_ship_mode() -> pd.DataFrame:
    """Build shipping mode dimension with derived attributes."""
    ship_data = [
        {"ship_mode": "Same Day", "ship_category": "Express", "avg_ship_days": 0},
        {"ship_mode": "First Class", "ship_category": "Express", "avg_ship_days": 2},
        {"ship_mode": "Second Class", "ship_category": "Standard", "avg_ship_days": 4},
        {"ship_mode": "Standard Class", "ship_category": "Standard", "avg_ship_days": 6},
    ]

    dim_ship_mode = pd.DataFrame(ship_data)
    dim_ship_mode.insert(0, "ship_mode_key", range(1, len(dim_ship_mode) + 1))
    return dim_ship_mode


def build_fact_sales(
    df: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_ship_mode: pd.DataFrame,
    config_path: str = "config.yaml",
) -> pd.DataFrame:
    """Build fact table with surrogate key lookups and derived metrics."""
    config = load_config(config_path)
    transform_cfg = config.get("transform", {})
    discount_threshold = transform_cfg.get("discount_threshold", 0.0)

    fact = df.copy()

    # ── Surrogate key lookups ──

    # Customer key
    customer_map = dim_customer.set_index("customer_id")["customer_key"].to_dict()
    fact["customer_key"] = fact["customer_id"].map(customer_map)

    # Product key
    product_map = dim_product.set_index("product_id")["product_key"].to_dict()
    fact["product_key"] = fact["product_id"].map(product_map)

    # Date key (order date)
    date_map = dim_date.set_index("full_date")["date_key"].to_dict()
    fact["order_date_normalized"] = pd.to_datetime(fact["order_date"]).dt.normalize()
    fact["date_key"] = fact["order_date_normalized"].map(date_map)

    # Ship date key
    fact["ship_date_normalized"] = pd.to_datetime(fact["ship_date"]).dt.normalize()
    fact["ship_date_key"] = fact["ship_date_normalized"].map(date_map)

    # Ship mode key
    ship_mode_map = dim_ship_mode.set_index("ship_mode")["ship_mode_key"].to_dict()
    fact["ship_mode_key"] = fact["ship_mode"].map(ship_mode_map)

    # ── Derived fields ──
    fact["sales_amount"] = fact["sales"]
    fact["unit_price"] = round(fact["sales_amount"] / fact["quantity"].replace(0, np.nan), 2)
    fact["discount_flag"] = (fact["discount"] > discount_threshold).astype(int)
    fact["return_flag"] = 0  # Placeholder — no returns in synthetic data

    # ── Select final columns ──
    fact_cols = [
        "order_id", "customer_key", "product_key", "date_key",
        "ship_date_key", "ship_mode_key", "quantity", "unit_price",
        "discount", "sales_amount", "profit", "shipping_cost",
        "discount_flag", "return_flag",
    ]

    fact_sales = fact[fact_cols].reset_index(drop=True)
    fact_sales.insert(0, "sale_key", range(1, len(fact_sales) + 1))

    return fact_sales


def transform(df: pd.DataFrame, config_path: str = "config.yaml") -> dict:
    """Run all transformations and return dimension/fact tables.

    Args:
        df: Validated raw data from extract phase.
        config_path: Path to pipeline config.

    Returns:
        Dictionary of table_name → DataFrame.
    """
    print(f"\n{'='*60}")
    print("TRANSFORM PHASE")
    print(f"{'='*60}")

    # Build dimensions
    print("Building dim_date...")
    dim_date = build_dim_date(config_path)
    print(f"  dim_date: {len(dim_date):,} rows ({dim_date['year'].min()}-{dim_date['year'].max()})")

    print("Building dim_customer...")
    dim_customer = build_dim_customer(df)
    print(f"  dim_customer: {len(dim_customer):,} rows")

    print("Building dim_product...")
    dim_product = build_dim_product(df)
    print(f"  dim_product: {len(dim_product):,} rows")

    print("Building dim_ship_mode...")
    dim_ship_mode = build_dim_ship_mode()
    print(f"  dim_ship_mode: {len(dim_ship_mode):,} rows")

    # Build fact table
    print("Building fact_sales...")
    fact_sales = build_fact_sales(df, dim_customer, dim_product, dim_date, dim_ship_mode, config_path)
    print(f"  fact_sales: {len(fact_sales):,} rows")

    # Check for unmapped keys
    unmapped_customers = fact_sales["customer_key"].isna().sum()
    unmapped_products = fact_sales["product_key"].isna().sum()
    unmapped_dates = fact_sales["date_key"].isna().sum()
    unmapped_ship_dates = fact_sales["ship_date_key"].isna().sum()

    if any([unmapped_customers, unmapped_products, unmapped_dates, unmapped_ship_dates]):
        print(f"\n  ⚠ Unmapped keys detected:")
        if unmapped_customers: print(f"    customer_key: {unmapped_customers}")
        if unmapped_products: print(f"    product_key: {unmapped_products}")
        if unmapped_dates: print(f"    date_key: {unmapped_dates}")
        if unmapped_ship_dates: print(f"    ship_date_key: {unmapped_ship_dates}")
    else:
        print(f"\n  All surrogate keys mapped successfully ✓")

    # Save processed CSVs
    config = load_config(config_path)
    processed_dir = config["paths"]["processed_data"]
    os.makedirs(processed_dir, exist_ok=True)

    tables = {
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_ship_mode": dim_ship_mode,
        "fact_sales": fact_sales,
    }

    print(f"\nSaving processed tables to {processed_dir}/")
    for name, table in tables.items():
        path = os.path.join(processed_dir, f"{name}.csv")
        table.to_csv(path, index=False)
        print(f"  {name}.csv ({len(table):,} rows)")

    return tables


if __name__ == "__main__":
    from src.extract import extract
    valid_df, _ = extract()
    tables = transform(valid_df)
