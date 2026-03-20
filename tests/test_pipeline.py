"""
Unit Tests for Retail Data Warehouse ETL Pipeline.

Tests cover each pipeline stage independently and the end-to-end flow.
"""

import os
import sys
import tempfile
import shutil

import sqlite3
import pandas as pd
import pytest
import yaml

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generate_data import generate_customers, generate_products, generate_orders, generate_data
from src.extract import extract
from src.transform import build_dim_date, build_dim_customer, build_dim_product, build_dim_ship_mode, transform
from src.load import load
from src.quality_checks import run_all_checks

import random
import numpy as np


# ── Fixtures ──

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test artifacts."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def test_config(temp_dir):
    """Create a minimal test configuration."""
    config = {
        "pipeline": {"name": "Test Pipeline", "version": "1.0.0"},
        "paths": {
            "raw_data": os.path.join(temp_dir, "raw"),
            "processed_data": os.path.join(temp_dir, "processed"),
            "warehouse": os.path.join(temp_dir, "test.db"),
            "quarantine": os.path.join(temp_dir, "quarantine"),
            "logs": os.path.join(temp_dir, "logs"),
        },
        "extract": {
            "source_file": "test_sales.csv",
            "date_columns": ["order_date", "ship_date"],
            "required_columns": ["order_id", "order_date", "customer_id", "product_id", "sales", "quantity", "profit"],
            "numeric_columns": ["sales", "quantity", "discount", "profit"],
        },
        "transform": {
            "date_range": {"start": "2023-01-01", "end": "2024-12-31"},
            "fiscal_year_start_month": 4,
            "discount_threshold": 0.0,
        },
        "load": {"batch_size": 1000, "replace_existing": True},
        "quality": {
            "completeness": {
                "max_null_pct": 0.0,
                "critical_columns": ["order_id", "customer_key", "product_key", "sales_amount"],
            },
            "uniqueness": {
                "primary_keys": {
                    "dim_customer": "customer_key",
                    "dim_product": "product_key",
                    "dim_date": "date_key",
                    "dim_ship_mode": "ship_mode_key",
                    "fact_sales": "sale_key",
                }
            },
            "referential_integrity": {
                "fact_sales": {
                    "customer_key": "dim_customer.customer_key",
                    "product_key": "dim_product.product_key",
                    "date_key": "dim_date.date_key",
                    "ship_date_key": "dim_date.date_key",
                    "ship_mode_key": "dim_ship_mode.ship_mode_key",
                }
            },
            "range_checks": {
                "quantity": {"min": 1, "max": 1000},
                "discount": {"min": 0.0, "max": 1.0},
                "sales_amount": {"min": 0.01},
            },
        },
        "data_generation": {"num_orders": 500, "num_customers": 50, "num_products": 100, "seed": 42},
    }

    config_path = os.path.join(temp_dir, "config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return config_path


# ── Data Generation Tests ──

class TestDataGeneration:
    def test_generate_customers(self):
        rng = random.Random(42)
        customers = generate_customers(50, rng)
        assert len(customers) == 50
        assert customers["customer_id"].is_unique
        assert not customers["customer_name"].isna().any()
        assert set(customers["segment"].unique()).issubset({"Consumer", "Corporate", "Home Office"})

    def test_generate_products(self):
        rng = random.Random(42)
        products = generate_products(100, rng)
        assert len(products) == 100
        assert products["product_id"].is_unique
        assert set(products["category"].unique()).issubset({"Furniture", "Office Supplies", "Technology"})

    def test_generate_orders(self):
        rng = random.Random(42)
        np_rng = np.random.default_rng(42)
        customers = generate_customers(20, rng)
        products = generate_products(50, rng)
        orders = generate_orders(200, customers, products, rng, np_rng,
                                 date_start="2023-01-01", date_end="2024-12-31")
        assert len(orders) == 200
        assert (orders["quantity"] > 0).all()
        assert (orders["sales"] > 0).all()

    def test_generate_data_creates_csv(self, test_config):
        path = generate_data(test_config)
        assert os.path.exists(path)
        df = pd.read_csv(path)
        assert len(df) == 500


# ── Extract Tests ──

class TestExtract:
    def test_extract_returns_valid_records(self, test_config):
        generate_data(test_config)
        valid_df, quarantined_df = extract(test_config)
        assert len(valid_df) > 0
        assert "order_id" in valid_df.columns

    def test_extract_quarantines_bad_records(self, test_config, temp_dir):
        # Generate data then inject bad rows
        generate_data(test_config)
        with open(test_config) as f:
            config = yaml.safe_load(f)
        raw_path = os.path.join(config["paths"]["raw_data"], config["extract"]["source_file"])
        df = pd.read_csv(raw_path)

        # Add a row with null order_id
        bad_row = df.iloc[0].copy()
        bad_row["order_id"] = None
        df = pd.concat([df, pd.DataFrame([bad_row])], ignore_index=True)
        df.to_csv(raw_path, index=False)

        valid_df, quarantined_df = extract(test_config)
        assert len(quarantined_df) >= 1


# ── Transform Tests ──

class TestTransform:
    def test_dim_date_coverage(self, test_config):
        dim_date = build_dim_date(test_config)
        assert len(dim_date) > 0
        assert dim_date["date_key"].is_unique
        assert "fiscal_quarter" in dim_date.columns
        assert dim_date["fiscal_quarter"].between(1, 4).all()

    def test_dim_customer_unique(self, test_config):
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        dim_customer = build_dim_customer(valid_df)
        assert dim_customer["customer_key"].is_unique
        assert dim_customer["customer_id"].is_unique

    def test_dim_product_unique(self, test_config):
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        dim_product = build_dim_product(valid_df)
        assert dim_product["product_key"].is_unique

    def test_dim_ship_mode(self):
        dim_ship = build_dim_ship_mode()
        assert len(dim_ship) == 4
        assert dim_ship["ship_mode_key"].is_unique

    def test_full_transform(self, test_config):
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        tables = transform(valid_df, test_config)
        assert set(tables.keys()) == {"dim_date", "dim_customer", "dim_product", "dim_ship_mode", "fact_sales"}
        assert len(tables["fact_sales"]) > 0


# ── Load Tests ──

class TestLoad:
    def test_load_creates_warehouse(self, test_config):
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        tables = transform(valid_df, test_config)
        con = load(tables, test_config)

        # Verify tables exist
        cursor = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_list = cursor.fetchall()
        assert len(table_list) == 5
        con.close()


# ── Quality Check Tests ──

class TestQualityChecks:
    def test_all_checks_pass(self, test_config):
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        tables = transform(valid_df, test_config)
        con = load(tables, test_config)
        results = run_all_checks(con, test_config)

        failed = [r for r in results if not r.passed]
        assert len(failed) == 0, f"Failed checks: {failed}"
        con.close()


# ── End-to-End Test ──

class TestEndToEnd:
    def test_full_pipeline(self, test_config):
        """Run the complete pipeline and verify warehouse integrity."""
        generate_data(test_config)
        valid_df, _ = extract(test_config)
        tables = transform(valid_df, test_config)
        con = load(tables, test_config)

        # Verify row counts match
        for table_name, df in tables.items():
            db_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert db_count == len(df), f"{table_name}: expected {len(df)}, got {db_count}"

        # Verify a sample query works
        result = pd.read_sql("""
            SELECT c.segment, SUM(f.sales_amount) AS total
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.segment
            ORDER BY total DESC
        """, con)

        assert len(result) > 0
        assert result["total"].sum() > 0

        con.close()
