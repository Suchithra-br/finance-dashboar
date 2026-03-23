# Finance Dashboard — Personal Finance Data Pipeline & Warehouse

A production-style personal finance dashboard built with:
- **DuckDB** — embedded analytical data warehouse
- **pandas** — data transformation layer
- **Streamlit** — interactive dashboard UI
- **Plotly** — charts (bar + donut)

---

## Project Structure

```
finance-dashboard/
├── data/
│   └── transactions.csv        ← raw transaction data (your source)
├── pipeline/
│   ├── extract.py              ← E: reads & validates CSV
│   ├── transform.py            ← T: builds fact + dimension tables
│   └── load.py                 ← L: writes to DuckDB warehouse
├── warehouse/
│   ├── schema.sql              ← table definitions + analytical views
│   └── finance.duckdb          ← generated after running main.py
├── dashboard/
│   └── app.py                  ← Streamlit dashboard
├── main.py                     ← pipeline orchestrator
├── requirements.txt
└── README.md
```

---

## Setup & Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the ETL pipeline (Extract → Transform → Load)
```bash
python main.py
```
This reads `data/transactions.csv`, transforms the data, and loads it into the DuckDB warehouse at `warehouse/finance.duckdb`.

### 3. Launch the dashboard
```bash
streamlit run dashboard/app.py
```
Open your browser at **http://localhost:8501**

---

## Data Warehouse Schema

| Layer     | Object                | Description                          |
|-----------|-----------------------|--------------------------------------|
| Dimension | `dim_category`        | Category master (Income / Expense)   |
| Dimension | `dim_date`            | Date dimension (year/month/quarter)  |
| Fact      | `fact_transactions`   | One row per transaction              |
| View      | `vw_kpi_summary`      | Total income, expenses, net savings  |
| View      | `vw_monthly_summary`  | Monthly income vs expense breakdown  |
| View      | `vw_spending_by_category` | Expense totals per category      |

---

## Adding New Data

1. Append rows to `data/transactions.csv` in the same format:
   ```
   date,description,category,amount,type
   2024-04-01,Grocery Store,Groceries,-1800,debit
   2024-04-15,Salary Credit,Income,48000,credit
   ```
2. Re-run `python main.py` to refresh the warehouse.
3. The dashboard auto-reflects the new data on next load.

---

## Dashboard Features

- **KPI Cards** — Total Income, Total Expenses, Net Savings, Transactions
- **Bar Chart** — Monthly Income vs Expenses (grouped)
- **Donut Chart** — Spending breakdown by category
- **Transaction Table** — Filterable by month and category
- **Sidebar Filters** — Filter by month and expense category
