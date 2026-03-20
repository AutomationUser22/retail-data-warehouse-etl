"""
Load Module — Load transformed data into SQLite analytical warehouse.

Responsibilities:
- Create/replace warehouse tables with proper DDL
- Enforce primary key and foreign key constraints
- Load dimension tables first, then fact table
- Log load statistics and timing

Note: Uses SQLite for zero-dependency portability. For production workloads,
swap to DuckDB or Redshift with minimal code changes (DDL + connection only).
"""

import os
import sqlite3
import time

import pandas as pd
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Table DDL definitions ──

DDL_STATEMENTS = {
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key        INTEGER PRIMARY KEY,
            full_date       TEXT NOT NULL,
            year            INTEGER NOT NULL,
            quarter         INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            month_name      TEXT NOT NULL,
            day_of_month    INTEGER NOT NULL,
            day_of_week     INTEGER NOT NULL,
            day_name        TEXT NOT NULL,
            week_of_year    INTEGER NOT NULL,
            is_weekend      INTEGER NOT NULL,
            fiscal_quarter  INTEGER NOT NULL,
            fiscal_year     INTEGER NOT NULL
        )
    """,
    "dim_customer": """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_key    INTEGER PRIMARY KEY,
            customer_id     TEXT NOT NULL UNIQUE,
            customer_name   TEXT NOT NULL,
            segment         TEXT NOT NULL,
            region          TEXT NOT NULL,
            country         TEXT NOT NULL,
            state           TEXT,
            city            TEXT,
            postal_code     TEXT
        )
    """,
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_key     INTEGER PRIMARY KEY,
            product_id      TEXT NOT NULL UNIQUE,
            product_name    TEXT NOT NULL,
            category        TEXT NOT NULL,
            sub_category    TEXT NOT NULL,
            manufacturer    TEXT NOT NULL
        )
    """,
    "dim_ship_mode": """
        CREATE TABLE IF NOT EXISTS dim_ship_mode (
            ship_mode_key   INTEGER PRIMARY KEY,
            ship_mode       TEXT NOT NULL UNIQUE,
            ship_category   TEXT NOT NULL,
            avg_ship_days   INTEGER NOT NULL
        )
    """,
    "fact_sales": """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_key        INTEGER PRIMARY KEY,
            order_id        TEXT NOT NULL,
            customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
            product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
            date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
            ship_date_key   INTEGER REFERENCES dim_date(date_key),
            ship_mode_key   INTEGER NOT NULL REFERENCES dim_ship_mode(ship_mode_key),
            quantity        INTEGER NOT NULL,
            unit_price      REAL NOT NULL,
            discount        REAL NOT NULL DEFAULT 0,
            sales_amount    REAL NOT NULL,
            profit          REAL NOT NULL,
            shipping_cost   REAL NOT NULL DEFAULT 0,
            discount_flag   INTEGER NOT NULL DEFAULT 0,
            return_flag     INTEGER NOT NULL DEFAULT 0
        )
    """,
}

# Load order: dimensions first, then facts
LOAD_ORDER = ["dim_date", "dim_customer", "dim_product", "dim_ship_mode", "fact_sales"]


def load_table(con: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> dict:
    """Load a single table into SQLite.

    Returns:
        Dictionary with load statistics.
    """
    start_time = time.time()

    cursor = con.cursor()

    # Drop and recreate table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(DDL_STATEMENTS[table_name])

    # Insert data using pandas to_sql for efficiency
    df.to_sql(table_name, con, if_exists="append", index=False)

    # Verify row count
    result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    loaded_rows = result[0]

    con.commit()
    elapsed = time.time() - start_time

    return {
        "table": table_name,
        "rows_loaded": loaded_rows,
        "rows_expected": len(df),
        "match": loaded_rows == len(df),
        "elapsed_seconds": round(elapsed, 3),
    }


def load(tables: dict, config_path: str = "config.yaml") -> sqlite3.Connection:
    """Load all tables into SQLite warehouse.

    Args:
        tables: Dictionary of table_name → DataFrame from transform phase.
        config_path: Path to pipeline config.

    Returns:
        SQLite connection to the loaded warehouse.
    """
    config = load_config(config_path)
    warehouse_path = config["paths"]["warehouse"]

    print(f"\n{'='*60}")
    print("LOAD PHASE")
    print(f"{'='*60}")
    print(f"Warehouse: {warehouse_path}")

    # Remove existing warehouse if replacing
    if config["load"].get("replace_existing", True) and os.path.exists(warehouse_path):
        os.remove(warehouse_path)
        print("Existing warehouse removed.")

    # Connect to SQLite with foreign key enforcement
    con = sqlite3.connect(warehouse_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")

    # Load tables in dependency order
    load_stats = []
    total_start = time.time()

    for table_name in LOAD_ORDER:
        if table_name not in tables:
            print(f"  ⚠ Skipping {table_name} — not found in transform output")
            continue

        stats = load_table(con, table_name, tables[table_name])
        load_stats.append(stats)

        status = "✓" if stats["match"] else "✗ MISMATCH"
        print(f"  {table_name}: {stats['rows_loaded']:,} rows loaded in {stats['elapsed_seconds']}s {status}")

    total_elapsed = round(time.time() - total_start, 3)

    # ── Load summary ──
    total_rows = sum(s["rows_loaded"] for s in load_stats)
    all_matched = all(s["match"] for s in load_stats)

    print(f"\nLoad Summary:")
    print(f"  Tables loaded:   {len(load_stats)}")
    print(f"  Total rows:      {total_rows:,}")
    print(f"  Total time:      {total_elapsed}s")
    print(f"  All counts match: {'✓' if all_matched else '✗ CHECK LOGS'}")

    # Verify warehouse file size
    if os.path.exists(warehouse_path):
        size_mb = os.path.getsize(warehouse_path) / (1024 * 1024)
        print(f"  Warehouse size:  {size_mb:.2f} MB")

    return con


if __name__ == "__main__":
    from src.extract import extract
    from src.transform import transform

    valid_df, _ = extract()
    tables = transform(valid_df)
    con = load(tables)
    con.close()
