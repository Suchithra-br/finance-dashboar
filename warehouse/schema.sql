-- ============================================================
-- Finance Data Warehouse Schema (DuckDB)
-- ============================================================

-- Dimension: Categories
CREATE TABLE IF NOT EXISTS dim_category (
    category_id   INTEGER PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    category_type VARCHAR NOT NULL   -- 'income' | 'expense'
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id    DATE PRIMARY KEY,
    year       INTEGER,
    month      INTEGER,
    month_name VARCHAR,
    quarter    INTEGER,
    day        INTEGER,
    weekday    VARCHAR
);

-- Fact: Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id   INTEGER PRIMARY KEY,
    date_id          DATE REFERENCES dim_date(date_id),
    category_id      INTEGER REFERENCES dim_category(category_id),
    description      VARCHAR,
    amount           DECIMAL(12, 2),  -- always positive
    transaction_type VARCHAR          -- 'credit' | 'debit'
);

-- ============================================================
-- Analytical Views
-- ============================================================

-- Monthly income vs expenses summary
CREATE OR REPLACE VIEW vw_monthly_summary AS
SELECT
    STRFTIME(date_id, '%Y-%m')                                          AS month,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)  AS total_income,
    SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END)  AS total_expenses,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
  - SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END)  AS net_savings
FROM fact_transactions
GROUP BY month
ORDER BY month;

-- Spending by category (expenses only)
CREATE OR REPLACE VIEW vw_spending_by_category AS
SELECT
    c.category_name,
    SUM(t.amount) AS total_spent
FROM fact_transactions t
JOIN dim_category c ON t.category_id = c.category_id
WHERE t.transaction_type = 'debit'
GROUP BY c.category_name
ORDER BY total_spent DESC;

-- Overall KPI summary
CREATE OR REPLACE VIEW vw_kpi_summary AS
SELECT
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END) AS total_income,
    SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END) AS total_expenses,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
  - SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END) AS net_savings,
    COUNT(*)                                                            AS total_transactions
FROM fact_transactions;
