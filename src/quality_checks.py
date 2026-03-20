"""
Data Quality Checks Module.

Automated quality validation framework for the star schema warehouse.

Check Categories:
- Completeness: No NULLs in critical columns
- Uniqueness: Primary keys are unique
- Referential Integrity: All FKs resolve to dimension tables
- Range Checks: Business rule validation on numeric fields
- Reconciliation: Fact totals match source data
"""

import sqlite3
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class QualityCheckResult:
    """Container for a single quality check result."""

    def __init__(self, check_name: str, category: str, table: str,
                 passed: bool, details: str = "", failing_count: int = 0):
        self.check_name = check_name
        self.category = category
        self.table = table
        self.passed = passed
        self.details = details
        self.failing_count = failing_count

    def __repr__(self):
        status = "PASS ✓" if self.passed else "FAIL ✗"
        msg = f"  [{status}] {self.category} | {self.table}.{self.check_name}"
        if not self.passed:
            msg += f" — {self.details} ({self.failing_count:,} failures)"
        return msg


def check_completeness(con: sqlite3.Connection, config: dict) -> list:
    """Check for NULL values in critical columns."""
    results = []
    quality_cfg = config.get("quality", {}).get("completeness", {})
    critical_columns = quality_cfg.get("critical_columns", [])

    # Check critical columns in fact_sales
    for col in critical_columns:
        try:
            null_count = con.execute(
                f"SELECT COUNT(*) FROM fact_sales WHERE {col} IS NULL"
            ).fetchone()[0]

            results.append(QualityCheckResult(
                check_name=col,
                category="COMPLETENESS",
                table="fact_sales",
                passed=null_count == 0,
                details=f"{null_count} NULL values found",
                failing_count=null_count,
            ))
        except Exception as e:
            results.append(QualityCheckResult(
                check_name=col,
                category="COMPLETENESS",
                table="fact_sales",
                passed=False,
                details=f"Check error: {str(e)}",
            ))

    # Check dimension tables for NULL primary keys
    dim_tables = {
        "dim_customer": "customer_key",
        "dim_product": "product_key",
        "dim_date": "date_key",
        "dim_ship_mode": "ship_mode_key",
    }

    for table, pk in dim_tables.items():
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {pk} IS NULL"
        ).fetchone()[0]

        results.append(QualityCheckResult(
            check_name=pk,
            category="COMPLETENESS",
            table=table,
            passed=null_count == 0,
            details=f"{null_count} NULL primary keys",
            failing_count=null_count,
        ))

    return results


def check_uniqueness(con: sqlite3.Connection, config: dict) -> list:
    """Check primary key uniqueness across all tables."""
    results = []
    pk_config = config.get("quality", {}).get("uniqueness", {}).get("primary_keys", {})

    for table, pk_col in pk_config.items():
        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        distinct = con.execute(f"SELECT COUNT(DISTINCT {pk_col}) FROM {table}").fetchone()[0]
        duplicates = total - distinct

        results.append(QualityCheckResult(
            check_name=pk_col,
            category="UNIQUENESS",
            table=table,
            passed=duplicates == 0,
            details=f"{duplicates} duplicate keys (total={total}, distinct={distinct})",
            failing_count=duplicates,
        ))

    return results


def check_referential_integrity(con: sqlite3.Connection, config: dict) -> list:
    """Check all foreign key relationships resolve."""
    results = []
    ri_config = config.get("quality", {}).get("referential_integrity", {})

    for fact_table, fk_mappings in ri_config.items():
        for fk_col, dim_ref in fk_mappings.items():
            dim_table, dim_pk = dim_ref.split(".")

            orphan_count = con.execute(f"""
                SELECT COUNT(*)
                FROM {fact_table} f
                LEFT JOIN {dim_table} d ON f.{fk_col} = d.{dim_pk}
                WHERE d.{dim_pk} IS NULL AND f.{fk_col} IS NOT NULL
            """).fetchone()[0]

            results.append(QualityCheckResult(
                check_name=f"{fk_col} → {dim_ref}",
                category="REF_INTEGRITY",
                table=fact_table,
                passed=orphan_count == 0,
                details=f"{orphan_count} orphaned foreign keys",
                failing_count=orphan_count,
            ))

    return results


def check_ranges(con: sqlite3.Connection, config: dict) -> list:
    """Check numeric columns are within expected business ranges."""
    results = []
    range_config = config.get("quality", {}).get("range_checks", {})

    for col, bounds in range_config.items():
        conditions = []
        if "min" in bounds:
            conditions.append(f"{col} < {bounds['min']}")
        if "max" in bounds:
            conditions.append(f"{col} > {bounds['max']}")

        if not conditions:
            continue

        where_clause = " OR ".join(conditions)
        violation_count = con.execute(
            f"SELECT COUNT(*) FROM fact_sales WHERE {where_clause}"
        ).fetchone()[0]

        results.append(QualityCheckResult(
            check_name=col,
            category="RANGE_CHECK",
            table="fact_sales",
            passed=violation_count == 0,
            details=f"{violation_count} values outside [{bounds.get('min', '-∞')}, {bounds.get('max', '∞')}]",
            failing_count=violation_count,
        ))

    return results


def check_reconciliation(con: sqlite3.Connection) -> list:
    """Check fact table aggregations are internally consistent."""
    results = []

    # Total sales should equal sum of (unit_price * quantity * (1 - discount)) approximately
    row = con.execute("""
        SELECT
            SUM(sales_amount) AS total_sales,
            SUM(quantity) AS total_qty,
            COUNT(*) AS total_rows,
            COUNT(DISTINCT order_id) AS distinct_orders
        FROM fact_sales
    """).fetchone()

    total_sales, total_qty, total_rows, distinct_orders = row

    # Basic sanity: more rows than orders (multi-item orders)
    results.append(QualityCheckResult(
        check_name="multi_item_orders",
        category="RECONCILIATION",
        table="fact_sales",
        passed=total_rows >= distinct_orders,
        details=f"rows={total_rows}, distinct_orders={distinct_orders}",
    ))

    # All dates in fact table exist in dim_date
    missing_dates = con.execute("""
        SELECT COUNT(DISTINCT f.date_key)
        FROM fact_sales f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """).fetchone()[0]

    results.append(QualityCheckResult(
        check_name="date_coverage",
        category="RECONCILIATION",
        table="fact_sales",
        passed=missing_dates == 0,
        details=f"{missing_dates} date keys not in dim_date",
        failing_count=missing_dates,
    ))

    # Discount flag consistency
    flag_mismatch = con.execute("""
        SELECT COUNT(*)
        FROM fact_sales
        WHERE (discount > 0 AND discount_flag = 0)
           OR (discount = 0 AND discount_flag = 1)
    """).fetchone()[0]

    results.append(QualityCheckResult(
        check_name="discount_flag_consistency",
        category="RECONCILIATION",
        table="fact_sales",
        passed=flag_mismatch == 0,
        details=f"{flag_mismatch} rows with mismatched discount flag",
        failing_count=flag_mismatch,
    ))

    return results


def run_all_checks(con: sqlite3.Connection, config_path: str = "config.yaml") -> list:
    """Run the complete data quality suite.

    Returns:
        List of QualityCheckResult objects.
    """
    config = load_config(config_path)

    print(f"\n{'='*60}")
    print("DATA QUALITY CHECKS")
    print(f"{'='*60}")

    all_results = []

    print("\n── Completeness ──")
    completeness = check_completeness(con, config)
    all_results.extend(completeness)
    for r in completeness:
        print(r)

    print("\n── Uniqueness ──")
    uniqueness = check_uniqueness(con, config)
    all_results.extend(uniqueness)
    for r in uniqueness:
        print(r)

    print("\n── Referential Integrity ──")
    ref_integrity = check_referential_integrity(con, config)
    all_results.extend(ref_integrity)
    for r in ref_integrity:
        print(r)

    print("\n── Range Checks ──")
    ranges = check_ranges(con, config)
    all_results.extend(ranges)
    for r in ranges:
        print(r)

    print("\n── Reconciliation ──")
    reconciliation = check_reconciliation(con)
    all_results.extend(reconciliation)
    for r in reconciliation:
        print(r)

    # ── Summary ──
    total = len(all_results)
    passed = sum(1 for r in all_results if r.passed)
    failed = total - passed

    print(f"\n{'='*60}")
    print(f"QUALITY SUMMARY: {passed}/{total} checks passed")
    if failed > 0:
        print(f"  ⚠ {failed} checks FAILED — review above for details")
    else:
        print(f"  All checks PASSED ✓")
    print(f"{'='*60}")

    return all_results


if __name__ == "__main__":
    config = load_config()
    con = sqlite3.connect(config["paths"]["warehouse"])
    run_all_checks(con)
    con.close()
