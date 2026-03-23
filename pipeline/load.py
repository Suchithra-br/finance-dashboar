"""
pipeline/load.py
----------------
LOAD layer — writes transformed DataFrames into the DuckDB
data warehouse and materialises the analytical views.
"""

import duckdb
import pandas as pd
from pathlib import Path


def get_connection(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database file and return the connection."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    print(f"[LOAD] Connected to warehouse: {db_path}")
    return conn


def run_schema(conn: duckdb.DuckDBPyConnection, schema_path: str | Path) -> None:
    """Execute the warehouse schema SQL file (creates tables & views)."""
    schema_path = Path(schema_path)
    sql = schema_path.read_text()
    # Execute each statement individually to avoid multi-statement issues
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    print("[LOAD] ✓ Schema applied.")


def load_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    mode: str = "replace",
) -> None:
    """
    Load a DataFrame into a DuckDB table.

    mode='replace' — drop existing rows and reload (full refresh)
    mode='append'  — insert new rows only
    """
    if mode == "replace":
        conn.execute(f"DELETE FROM {table_name}")

    # DuckDB can register a pandas DataFrame directly by name
    conn.register("_staging_df", df)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM _staging_df")
    conn.unregister("_staging_df")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"[LOAD] ✓ {table_name}: {row_count} rows loaded.")


def load(
    tables: dict[str, pd.DataFrame],
    db_path: str | Path,
    schema_path: str | Path,
) -> duckdb.DuckDBPyConnection:
    """
    Full load pipeline:
      1. Connect / create warehouse
      2. Apply schema
      3. Load dimension tables (order matters for FK integrity)
      4. Load fact table
    Returns the open connection (caller must close when done).
    """
    print("[LOAD] Starting load …")
    conn = get_connection(db_path)
    run_schema(conn, schema_path)

    # Delete fact first (it references dims via FK), then reload everything
    conn.execute("DELETE FROM fact_transactions")
    conn.execute("DELETE FROM dim_category")
    conn.execute("DELETE FROM dim_date")

    load_table(conn, "dim_category",      tables["dim_category"], mode="append")
    load_table(conn, "dim_date",          tables["dim_date"],      mode="append")
    load_table(conn, "fact_transactions", tables["fact_transactions"], mode="append")

    print("[LOAD] ✓ Data warehouse load complete.")
    return conn


def query(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Helper to run any analytical SQL and return a DataFrame."""
    return conn.execute(sql).df()
