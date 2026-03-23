"""
pipeline/transform.py
---------------------
TRANSFORM layer — cleans, enriches, and structures the raw
DataFrame into fact + dimension tables ready for the warehouse.
"""

import pandas as pd


# ── Category master ───────────────────────────────────────────────────────────
CATEGORY_MAP = {
    "Income":        ("Income",        "income"),
    "Groceries":     ("Groceries",     "expense"),
    "Food":          ("Food",          "expense"),
    "Transport":     ("Transport",     "expense"),
    "Utilities":     ("Utilities",     "expense"),
    "Shopping":      ("Shopping",      "expense"),
    "Health":        ("Health",        "expense"),
    "Entertainment": ("Entertainment", "expense"),
    "Other":         ("Other",         "expense"),
}


def build_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_category dimension table.
    Returns a DataFrame: category_id | category_name | category_type
    """
    categories = sorted(df["category"].unique())
    rows = []
    for idx, cat in enumerate(categories, start=1):
        name, ctype = CATEGORY_MAP.get(cat, (cat, "expense"))
        rows.append({"category_id": idx, "category_name": name, "category_type": ctype})

    dim_cat = pd.DataFrame(rows)
    print(f"[TRANSFORM] ✓ dim_category: {len(dim_cat)} rows")
    return dim_cat


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_date dimension table from all unique dates.
    Returns a DataFrame: date_id | year | month | month_name | quarter | day | weekday
    """
    unique_dates = df["date"].drop_duplicates().sort_values()
    dim_date = pd.DataFrame({
        "date_id":    unique_dates,
        "year":       unique_dates.dt.year,
        "month":      unique_dates.dt.month,
        "month_name": unique_dates.dt.strftime("%B"),
        "quarter":    unique_dates.dt.quarter,
        "day":        unique_dates.dt.day,
        "weekday":    unique_dates.dt.day_name(),
    }).reset_index(drop=True)

    print(f"[TRANSFORM] ✓ dim_date: {len(dim_date)} rows")
    return dim_date


def build_fact_transactions(
    df: pd.DataFrame,
    dim_category: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the fact_transactions table.
    - Normalises amount to always be positive (sign is encoded in transaction_type)
    - Joins on category to get category_id
    Returns a DataFrame matching the warehouse schema.
    """
    # Make amount always positive; debit/credit is captured in 'type'
    df = df.copy()
    df["amount"] = df["amount"].abs()

    # Rename 'type' → 'transaction_type' for clarity
    df = df.rename(columns={"type": "transaction_type", "date": "date_id"})

    # Resolve category_id via lookup
    cat_lookup = dim_category.set_index("category_name")["category_id"]
    df["category_id"] = df["category"].map(cat_lookup)

    # Auto-generate surrogate key
    df = df.reset_index(drop=True)
    df.insert(0, "transaction_id", df.index + 1)

    fact = df[["transaction_id", "date_id", "category_id",
               "description", "amount", "transaction_type"]].copy()

    # Sanity check
    total_income   = fact.loc[fact["transaction_type"] == "credit", "amount"].sum()
    total_expenses = fact.loc[fact["transaction_type"] == "debit",  "amount"].sum()
    print(f"[TRANSFORM] ✓ fact_transactions: {len(fact)} rows")
    print(f"[TRANSFORM]   Income ₹{total_income:,.0f}  |  "
          f"Expenses ₹{total_expenses:,.0f}  |  "
          f"Net ₹{total_income - total_expenses:,.0f}")
    return fact


def transform(raw_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Full transform pipeline.
    Returns a dict with keys: 'dim_category', 'dim_date', 'fact_transactions'
    """
    print("[TRANSFORM] Starting transformation …")
    dim_category      = build_dim_category(raw_df)
    dim_date          = build_dim_date(raw_df)
    fact_transactions = build_fact_transactions(raw_df, dim_category)
    print("[TRANSFORM] ✓ All tables built.")
    return {
        "dim_category":      dim_category,
        "dim_date":          dim_date,
        "fact_transactions": fact_transactions,
    }
