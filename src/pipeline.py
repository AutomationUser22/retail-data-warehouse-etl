"""
Pipeline Orchestrator — Runs the full ETL pipeline end-to-end.

Stages:
1. Generate synthetic data (if raw data doesn't exist)
2. Extract & validate raw data
3. Transform into star schema
4. Load into SQLite warehouse
5. Run data quality checks
6. Print sample analytical queries
"""

import argparse
import os
import sqlite3
import sys
import time

import pandas as pd

from src.generate_data import generate_data
from src.extract import extract
from src.transform import transform
from src.load import load
from src.quality_checks import run_all_checks


def run_sample_queries(con: sqlite3.Connection):
    """Run sample analytical queries to demonstrate the warehouse."""
    print(f"\n{'='*60}")
    print("SAMPLE ANALYTICAL QUERIES")
    print(f"{'='*60}")

    # Query 1: Top 10 customers by lifetime value
    print("\n── Top 10 Customers by Lifetime Value ──")
    results = pd.read_sql("""
        SELECT c.customer_name, c.segment, c.region,
               ROUND(SUM(f.sales_amount), 2) AS lifetime_value,
               COUNT(DISTINCT f.order_id) AS total_orders,
               ROUND(AVG(f.sales_amount), 2) AS avg_order_value
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_name, c.segment, c.region
        ORDER BY lifetime_value DESC
        LIMIT 10
    """, con)
    print(results.to_string(index=False))

    # Query 2: Monthly revenue trend
    print("\n── Monthly Revenue Trend (Last 2 Years) ──")
    results = pd.read_sql("""
        SELECT d.year, d.month, d.month_name,
               ROUND(SUM(f.sales_amount), 2) AS revenue,
               ROUND(SUM(f.profit), 2) AS profit,
               ROUND(SUM(f.profit) / NULLIF(SUM(f.sales_amount), 0) * 100, 1) AS margin_pct,
               COUNT(DISTINCT f.order_id) AS orders
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE d.year >= (SELECT MAX(year) - 1 FROM dim_date d2 
                         JOIN fact_sales f2 ON d2.date_key = f2.date_key)
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """, con)
    print(results.to_string(index=False))

    # Query 3: Product category performance
    print("\n── Product Category Performance ──")
    results = pd.read_sql("""
        SELECT p.category, p.sub_category,
               ROUND(SUM(f.sales_amount), 2) AS revenue,
               ROUND(SUM(f.profit), 2) AS profit,
               ROUND(AVG(f.discount) * 100, 1) AS avg_discount_pct,
               SUM(f.quantity) AS units_sold,
               ROUND(SUM(CASE WHEN f.discount_flag = 1 THEN f.sales_amount ELSE 0 END) /
                     NULLIF(SUM(f.sales_amount), 0) * 100, 1) AS pct_discounted_revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category, p.sub_category
        ORDER BY revenue DESC
    """, con)
    print(results.to_string(index=False))

    # Query 4: Shipping mode analysis
    print("\n── Shipping Mode Analysis ──")
    results = pd.read_sql("""
        SELECT sm.ship_mode, sm.ship_category, sm.avg_ship_days,
               COUNT(*) AS order_lines,
               ROUND(SUM(f.sales_amount), 2) AS revenue,
               ROUND(SUM(f.shipping_cost), 2) AS total_shipping_cost,
               ROUND(AVG(f.shipping_cost), 2) AS avg_shipping_cost
        FROM fact_sales f
        JOIN dim_ship_mode sm ON f.ship_mode_key = sm.ship_mode_key
        GROUP BY sm.ship_mode, sm.ship_category, sm.avg_ship_days
        ORDER BY order_lines DESC
    """, con)
    print(results.to_string(index=False))

    # Query 5: Regional quarterly performance with window function
    print("\n── Regional Quarterly Performance (with QoQ Growth) ──")
    results = pd.read_sql("""
        WITH quarterly AS (
            SELECT c.region, d.year, d.quarter,
                   ROUND(SUM(f.sales_amount), 2) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY c.region, d.year, d.quarter
        )
        SELECT region, year, quarter, revenue,
               ROUND((revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY year, quarter))
                     / NULLIF(LAG(revenue) OVER (PARTITION BY region ORDER BY year, quarter), 0) * 100, 1)
                     AS qoq_growth_pct
        FROM quarterly
        ORDER BY region, year, quarter
    """, con)
    print(results.to_string(index=False))


def main(config_path: str = "config.yaml"):
    """Run the complete ETL pipeline."""
    pipeline_start = time.time()

    print("╔════════════════════════════════════════════════════════════╗")
    print("║        RETAIL DATA WAREHOUSE — ETL PIPELINE              ║")
    print("╚════════════════════════════════════════════════════════════╝")

    # ── Stage 0: Generate data if needed ──
    import yaml
    with open(config_path) as f:
        config = yaml.safe_load(f)

    raw_file = os.path.join(config["paths"]["raw_data"], config["extract"]["source_file"])
    if not os.path.exists(raw_file):
        print("\nRaw data not found — generating synthetic dataset...")
        generate_data(config_path)
    else:
        print(f"\nRaw data found: {raw_file}")

    # ── Stage 1: Extract ──
    valid_df, quarantined_df = extract(config_path)

    # ── Stage 2: Transform ──
    tables = transform(valid_df, config_path)

    # ── Stage 3: Load ──
    con = load(tables, config_path)

    # ── Stage 4: Quality Checks ──
    results = run_all_checks(con, config_path)

    # ── Stage 5: Sample Queries ──
    run_sample_queries(con)

    # ── Pipeline Complete ──
    total_elapsed = round(time.time() - pipeline_start, 2)
    failed_checks = sum(1 for r in results if not r.passed)

    print(f"\n╔════════════════════════════════════════════════════════════╗")
    print(f"║  PIPELINE COMPLETE                                        ║")
    print(f"║  Total time: {total_elapsed}s{' ' * (44 - len(str(total_elapsed)))}║")
    print(f"║  Quality: {len(results) - failed_checks}/{len(results)} checks passed" +
          f"{' ' * (38 - len(str(len(results) - failed_checks)) - len(str(len(results))))}║")
    print(f"╚════════════════════════════════════════════════════════════╝")

    con.close()

    return 0 if failed_checks == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Data Warehouse ETL Pipeline")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    sys.exit(main(args.config))
