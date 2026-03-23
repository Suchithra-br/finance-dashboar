"""
pipeline/extract.py
-------------------
EXTRACT layer — reads raw transaction data from the source CSV
and returns a validated pandas DataFrame.
"""

import pandas as pd
from pathlib import Path


REQUIRED_COLUMNS = {"date", "description", "category", "amount", "type"}

VALID_TYPES = {"credit", "debit"}

VALID_CATEGORIES = {
    "Income", "Groceries", "Food", "Transport",
    "Utilities", "Shopping", "Health", "Entertainment", "Other",
}


def extract_from_csv(filepath: str | Path) -> pd.DataFrame:
    """
    Read transactions from a CSV file.

    Expected CSV columns:
        date         – YYYY-MM-DD
        description  – free text
        category     – one of VALID_CATEGORIES
        amount       – numeric (negative = debit, positive = credit)
        type         – 'credit' | 'debit'

    Returns a DataFrame with raw but type-cast data.
    Raises ValueError on schema or data issues.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Source file not found: {filepath}")

    print(f"[EXTRACT] Reading from {filepath} …")
    df = pd.read_csv(filepath)

    # ── Schema check ──────────────────────────────────────────────────────────
    missing = REQUIRED_COLUMNS - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    df.columns = df.columns.str.lower().str.strip()

    # ── Type casting ──────────────────────────────────────────────────────────
    df["date"]        = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["amount"]      = pd.to_numeric(df["amount"], errors="raise")
    df["description"] = df["description"].astype(str).str.strip()
    df["category"]    = df["category"].astype(str).str.strip()
    df["type"]        = df["type"].astype(str).str.strip().str.lower()

    # ── Data validation ───────────────────────────────────────────────────────
    bad_types = df[~df["type"].isin(VALID_TYPES)]
    if not bad_types.empty:
        raise ValueError(
            f"Unknown transaction types found:\n{bad_types[['date','description','type']]}"
        )

    null_counts = df.isnull().sum()
    if null_counts.any():
        print(f"[EXTRACT] ⚠ Null values detected:\n{null_counts[null_counts > 0]}")

    print(f"[EXTRACT] ✓ Extracted {len(df)} records — "
          f"{df['type'].value_counts().to_dict()}")
    return df


if __name__ == "__main__":
    # Quick smoke-test
    data_path = Path(__file__).parents[1] / "data" / "transactions.csv"
    df = extract_from_csv(data_path)
    print(df.head())
