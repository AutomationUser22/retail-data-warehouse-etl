"""
Extract Module — Read and validate raw source data.

Responsibilities:
- Load raw CSV files with schema validation
- Type casting and date parsing
- Null/missing value detection
- Quarantine malformed records
- Log extraction statistics
"""

import os
from datetime import datetime
from typing import Tuple

import pandas as pd
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def extract(config_path: str = "config.yaml") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract and validate raw data from CSV source.

    Returns:
        Tuple of (valid_records DataFrame, quarantined_records DataFrame)
    """
    config = load_config(config_path)
    extract_cfg = config["extract"]
    raw_dir = config["paths"]["raw_data"]
    source_file = os.path.join(raw_dir, extract_cfg["source_file"])

    print(f"\n{'='*60}")
    print("EXTRACT PHASE")
    print(f"{'='*60}")
    print(f"Source: {source_file}")

    # ── Read raw CSV ──
    df = pd.read_csv(source_file)
    total_rows = len(df)
    print(f"Raw rows loaded: {total_rows:,}")
    print(f"Columns: {list(df.columns)}")

    # ── Schema validation: required columns ──
    required = extract_cfg.get("required_columns", [])
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    print(f"Required columns present: {len(required)}/{len(required)} ✓")

    # ── Parse dates ──
    for col in extract_cfg.get("date_columns", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    print(f"Date columns parsed: {extract_cfg.get('date_columns', [])} ✓")

    # ── Cast numeric columns ──
    for col in extract_cfg.get("numeric_columns", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Identify quarantine records ──
    quarantine_mask = pd.Series(False, index=df.index)
    quarantine_reasons = pd.Series("", index=df.index)

    # Null check on required columns
    for col in required:
        null_mask = df[col].isna()
        if null_mask.any():
            quarantine_mask |= null_mask
            quarantine_reasons = quarantine_reasons.where(
                ~null_mask, quarantine_reasons + f"NULL_{col}; "
            )

    # Negative quantity check
    if "quantity" in df.columns:
        neg_qty = df["quantity"] <= 0
        quarantine_mask |= neg_qty
        quarantine_reasons = quarantine_reasons.where(
            ~neg_qty, quarantine_reasons + "NEGATIVE_QUANTITY; "
        )

    # Invalid date check
    for col in extract_cfg.get("date_columns", []):
        if col in df.columns:
            bad_dates = df[col].isna()  # Failed to parse
            quarantine_mask |= bad_dates
            quarantine_reasons = quarantine_reasons.where(
                ~bad_dates, quarantine_reasons + f"INVALID_DATE_{col}; "
            )

    # ── Split valid vs quarantined ──
    quarantined = df[quarantine_mask].copy()
    quarantined["quarantine_reason"] = quarantine_reasons[quarantine_mask]
    quarantined["quarantine_timestamp"] = datetime.now().isoformat()

    valid = df[~quarantine_mask].copy()

    # ── Save quarantine file ──
    quarantine_dir = config["paths"].get("quarantine", "data/quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    if len(quarantined) > 0:
        quarantine_path = os.path.join(quarantine_dir, "quarantined_records.csv")
        quarantined.to_csv(quarantine_path, index=False)
        print(f"Quarantined records: {len(quarantined):,} → {quarantine_path}")
    else:
        print("Quarantined records: 0 ✓")

    # ── Extraction stats ──
    print(f"\nExtraction Summary:")
    print(f"  Total rows:      {total_rows:,}")
    print(f"  Valid rows:      {len(valid):,}")
    print(f"  Quarantined:     {len(quarantined):,}")
    print(f"  Pass rate:       {len(valid)/total_rows*100:.1f}%")

    # Column-level null report
    null_counts = valid.isnull().sum()
    if null_counts.any():
        print(f"\n  Null counts (valid records):")
        for col, count in null_counts[null_counts > 0].items():
            print(f"    {col}: {count:,}")

    return valid, quarantined


if __name__ == "__main__":
    valid_df, quarantined_df = extract()
    print(f"\nValid data shape: {valid_df.shape}")
