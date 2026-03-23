"""
main.py
-------
Pipeline orchestrator — runs Extract → Transform → Load in sequence.

Usage:
    python main.py

After running, launch the dashboard:
    streamlit run dashboard/app.py
"""

from pathlib import Path

from pipeline.extract   import extract_from_csv
from pipeline.transform import transform
from pipeline.load      import load, query


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
DATA_FILE    = BASE_DIR / "data"      / "transactions.csv"
DB_FILE      = BASE_DIR / "warehouse" / "finance.duckdb"
SCHEMA_FILE  = BASE_DIR / "warehouse" / "schema.sql"


def run_pipeline() -> None:
    print("=" * 60)
    print("  FINANCE DATA PIPELINE  —  Jan to Mar 2024")
    print("=" * 60)

    # 1. EXTRACT
    raw_df = extract_from_csv(DATA_FILE)

    # 2. TRANSFORM
    tables = transform(raw_df)

    # 3. LOAD
    conn = load(tables, DB_FILE, SCHEMA_FILE)

    # 4. Quick verification queries
    print("\n── KPI Summary ──────────────────────────────────────────")
    kpi = query(conn, "SELECT * FROM vw_kpi_summary")
    print(kpi.to_string(index=False))

    print("\n── Monthly Summary ──────────────────────────────────────")
    monthly = query(conn, "SELECT * FROM vw_monthly_summary")
    print(monthly.to_string(index=False))

    print("\n── Spending by Category ─────────────────────────────────")
    spending = query(conn, "SELECT * FROM vw_spending_by_category")
    print(spending.to_string(index=False))

    conn.close()
    print("\n✅ Pipeline complete. Run:  streamlit run dashboard/app.py")


if __name__ == "__main__":
    run_pipeline()
