"""
dashboard/app.py
Run: python -m streamlit run dashboard/app.py
"""

import calendar
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import date, datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
DB_FILE = Path(__file__).parents[1] / "warehouse" / "finance.duckdb"

st.set_page_config(page_title="Finance Dashboard", page_icon="💰", layout="wide")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .stApp { background-color: #f4f6fb; }
  .block-container { padding-top: 48px; padding-bottom: 32px; }
  header[data-testid="stHeader"] { background: transparent; }

  /* Sidebar white theme */
  [data-testid="stSidebar"] { background-color: #ffffff !important; }
  [data-testid="stSidebar"] * { color: #1f2937 !important; }
  [data-testid="stSidebar"] input {
    background-color: #f9fafb !important;
    color: #111827 !important;
    border: 1.5px solid #d1d5db !important;
    border-radius: 8px !important;
    font-size: 14px !important;
    font-weight: 600 !important;
  }
  [data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #f9fafb !important;
    border: 1.5px solid #d1d5db !important;
    border-radius: 8px !important;
  }
  [data-testid="stSidebar"] [data-baseweb="tag"] {
    background-color: #dbeafe !important;
    border-radius: 6px !important;
  }
  [data-testid="stSidebar"] [data-baseweb="tag"] span { color: #1e40af !important; }
  [data-baseweb="popover"], [data-baseweb="popover"] * {
    background-color: #ffffff !important;
    color: #1f2937 !important;
  }
  [data-testid="stSidebar"] label {
    font-weight: 700 !important;
    font-size: 13px !important;
    color: #374151 !important;
  }

  /* KPI Cards */
  .kpi-card {
    background: #ffffff; border-radius: 16px;
    padding: 20px 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.07);
  }
  .kpi-label {
    font-size: 11px; font-weight: 700; letter-spacing: 0.1em;
    color: #6b7280; text-transform: uppercase; margin-bottom: 6px;
  }
  .kpi-value { font-size: 26px; font-weight: 800; color: #111827; margin: 0; }
  .kpi-value.green { color: #059669; }
  .kpi-value.red   { color: #dc2626; }
  .kpi-value.blue  { color: #2563eb; }

  /* Chart boxes */
  .chart-box {
    background: #ffffff; border-radius: 16px;
    padding: 22px 22px 10px 22px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07); margin-bottom: 16px;
  }
  .chart-title { font-size: 15px; font-weight: 700; color: #1f2937; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_FILE.exists():
        st.error("Warehouse not found. Run `python main.py` first.")
        st.stop()
    return duckdb.connect(str(DB_FILE), read_only=True)

def q(sql):
    return get_conn().execute(sql).df()

# ── Data bounds ───────────────────────────────────────────────────────────────
bounds   = q("SELECT MIN(date_id) AS mn, MAX(date_id) AS mx FROM fact_transactions").iloc[0]
DATA_MIN = bounds["mn"].date() if hasattr(bounds["mn"], "date") else date(2024, 1, 1)
DATA_MAX = bounds["mx"].date() if hasattr(bounds["mx"], "date") else date.today()

all_cats = q(
    "SELECT category_name FROM dim_category WHERE category_type='expense' ORDER BY category_name"
)["category_name"].tolist()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📅 Select Date Range")
    st.caption(f"Data: **{DATA_MIN.strftime('%d %b %Y')}** → **{DATA_MAX.strftime('%d %b %Y')}**")
    st.markdown("---")

    start_date = st.date_input(
        "From date", value=date(DATA_MIN.year, DATA_MIN.month, 1),
        min_value=date(2023, 1, 1), max_value=DATA_MAX, format="DD/MM/YYYY",
    )
    end_date = st.date_input(
        "To date", value=DATA_MAX,
        min_value=date(2023, 1, 1), max_value=DATA_MAX, format="DD/MM/YYYY",
    )
    st.markdown("---")

    selected_cats = st.multiselect("Expense Categories", all_cats, default=all_cats)
    st.markdown("---")

    if st.button("🔄 Refresh"):
        st.cache_resource.clear()
        st.rerun()

# ── Validation ────────────────────────────────────────────────────────────────
if start_date > end_date:
    st.error("'From date' must be before 'To date'.")
    st.stop()

# ── Greeting ─────────────────────────────────────────────────────────────────
hour     = datetime.now().hour
greeting = "🌅 Good Morning" if hour < 12 else ("☀️ Good Afternoon" if hour < 17 else "🌙 Good Evening")

st.markdown(f"""
<div style="padding:10px 0 4px 0;">
  <h1 style="font-size:30px;font-weight:800;color:#1f2937;margin:0;">{greeting}, Suchi! 👋</h1>
  <p style="font-size:15px;color:#6b7280;margin:4px 0 0 0;">Here's your personal financial snapshot 💼</p>
</div>
""", unsafe_allow_html=True)

# Date range badge
st.markdown(f"""
<div style="background:#e0f2fe;border-left:4px solid #0284c7;border-radius:8px;
            padding:10px 16px;margin:10px 0 16px 0;display:inline-block;">
  <span style="font-size:14px;font-weight:700;color:#0369a1;">
    📅 &nbsp; {start_date.strftime('%d %b %Y')} &nbsp;→&nbsp; {end_date.strftime('%d %b %Y')}
  </span>
</div>
""", unsafe_allow_html=True)

# ── Build SQL filters ─────────────────────────────────────────────────────────
sd       = start_date.strftime("%Y-%m-%d")
ed       = end_date.strftime("%Y-%m-%d")
cat_list = ", ".join(f"'{c}'" for c in selected_cats) if selected_cats else "''"
base_f   = f"t.date_id BETWEEN '{sd}' AND '{ed}'"

# ── KPI query ─────────────────────────────────────────────────────────────────
kpi = q(f"""
    SELECT
        SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END) AS income,
        SUM(CASE WHEN transaction_type='debit'  THEN amount ELSE 0 END) AS expenses,
        SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END)
      - SUM(CASE WHEN transaction_type='debit'  THEN amount ELSE 0 END) AS savings,
        COUNT(*) AS txns
    FROM fact_transactions t WHERE {base_f}
""").iloc[0]

# No data guard
if kpi["txns"] == 0:
    st.warning(
        f"No transactions found between **{start_date.strftime('%d %b %Y')}** "
        f"and **{end_date.strftime('%d %b %Y')}**. "
        f"Data starts from **{DATA_MIN.strftime('%d %b %Y')}**."
    )
    st.stop()

# Income = 0 hint
if kpi["income"] == 0:
    st.info("💡 Income shows ₹0 — salary credits on the **15th** of each month. "
            "Select a range that includes the 15th, or pick a full month.")

# ── KPI Cards ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)

def kpi_card(col, label, value, color=""):
    col.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <p class="kpi-value {color}">{value}</p>
    </div>""", unsafe_allow_html=True)

kpi_card(c1, "TOTAL INCOME",   f"₹{kpi['income']:,.0f}",   "green")
kpi_card(c2, "TOTAL EXPENSES", f"₹{kpi['expenses']:,.0f}",  "red")
kpi_card(c3, "NET SAVINGS",    f"₹{kpi['savings']:,.0f}",   "blue" if kpi['savings'] >= 0 else "red")
kpi_card(c4, "TRANSACTIONS",   f"{int(kpi['txns'])}")

st.markdown("<br>", unsafe_allow_html=True)

# ── Monthly summary ───────────────────────────────────────────────────────────
monthly = q(f"""
    SELECT
        STRFTIME(date_id,'%Y-%m') AS ym,
        YEAR(date_id)  AS yr,
        MONTH(date_id) AS mo,
        SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END) AS income,
        SUM(CASE WHEN transaction_type='debit'  THEN amount ELSE 0 END) AS expenses
    FROM fact_transactions t WHERE {base_f}
    GROUP BY ym, yr, mo ORDER BY ym
""")
monthly["label"] = monthly.apply(
    lambda r: f"{calendar.month_abbr[int(r.mo)]} {int(r.yr)}", axis=1
)

# ── Spending by category ──────────────────────────────────────────────────────
spending = q(f"""
    SELECT c.category_name, SUM(t.amount) AS total_spent
    FROM fact_transactions t
    JOIN dim_category c ON t.category_id = c.category_id
    WHERE t.transaction_type='debit' AND {base_f}
      {"AND c.category_name IN (" + cat_list + ")" if selected_cats else ""}
    GROUP BY c.category_name ORDER BY total_spent DESC
""")

# ── Charts row ────────────────────────────────────────────────────────────────
left, right = st.columns(2)

# Bar chart
with left:
    st.markdown('<div class="chart-box">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">📊 Monthly Income vs Expenses</div>', unsafe_allow_html=True)

    melted = monthly.melt(id_vars="label", value_vars=["income","expenses"],
                          var_name="Metric", value_name="Amount")
    melted["Metric"] = melted["Metric"].map({"income":"Income","expenses":"Expenses"})

    fig_bar = px.bar(
        melted, x="label", y="Amount", color="Metric", barmode="group",
        color_discrete_map={"Income":"#10b981","Expenses":"#f43f5e"},
    )
    fig_bar.update_traces(
        texttemplate="₹%{y:,.0f}", textposition="outside",
        textfont=dict(size=9), cliponaxis=False,
    )
    fig_bar.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=12, color="#1f2937"), bgcolor="white",
                    bordercolor="#e5e7eb", borderwidth=1),
        legend_title_text="",
        margin=dict(l=10, r=10, t=10, b=70), height=340,
        xaxis=dict(title="", tickangle=-45, tickfont=dict(size=11, color="#374151"),
                   gridcolor="#f3f4f6", showline=True, linecolor="#e5e7eb"),
        yaxis=dict(title="Amount (₹)", tickprefix="₹", tickformat=",.0f",
                   gridcolor="#f3f4f6", tickfont=dict(size=11, color="#374151"),
                   title_font=dict(color="#374151")),
        bargap=0.25, bargroupgap=0.08, font=dict(color="#1f2937"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# Pie chart
with right:
    st.markdown('<div class="chart-box">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">🥧 Spending by Category</div>', unsafe_allow_html=True)

    PALETTE = ["#3b82f6","#10b981","#f97316","#f43f5e",
               "#a78bfa","#06b6d4","#eab308","#64748b","#ec4899","#14b8a6"]

    if spending.empty:
        st.info("No expense data for selected filters.")
    else:
        labels_with_amt = [
            f"{r.category_name}  ₹{r.total_spent:,.0f}"
            for r in spending.itertuples()
        ]
        fig_pie = go.Figure(go.Pie(
            labels=labels_with_amt,
            values=spending["total_spent"],
            hole=0.50,
            marker=dict(colors=PALETTE[:len(spending)], line=dict(color="#fff", width=2)),
            textinfo="percent",
            textposition="inside",
            insidetextfont=dict(size=12, color="white"),
            hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>",
            pull=[0.03] * len(spending),
        ))
        fig_pie.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.01, y=0.5, xanchor="left",
                        font=dict(size=11, color="#1f2937"),
                        bgcolor="white", bordercolor="#e5e7eb", borderwidth=1),
            margin=dict(l=20, r=200, t=20, b=20),
            paper_bgcolor="white", height=360,
            font=dict(color="#1f2937"),
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ── Trend line ────────────────────────────────────────────────────────────────
st.markdown('<div class="chart-box">', unsafe_allow_html=True)
st.markdown('<div class="chart-title">📈 Monthly Spending Trend</div>', unsafe_allow_html=True)

fig_trend = px.line(
    monthly, x="label", y=["income","expenses"],
    color_discrete_map={"income":"#10b981","expenses":"#f43f5e"},
    markers=True, labels={"label":"","value":"Amount (₹)","variable":""},
)
fig_trend.for_each_trace(lambda t: t.update(
    name="Income" if t.name == "income" else "Expenses"
))
fig_trend.update_layout(
    plot_bgcolor="white", paper_bgcolor="white",
    margin=dict(l=10, r=10, t=10, b=60), height=260,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                font=dict(size=12, color="#1f2937"), bgcolor="white"),
    xaxis=dict(tickangle=-45, tickfont=dict(size=11, color="#374151"), gridcolor="#f3f4f6"),
    yaxis=dict(tickprefix="₹", tickformat=",.0f", gridcolor="#f3f4f6",
               tickfont=dict(size=11, color="#374151")),
    font=dict(color="#1f2937"),
)
fig_trend.update_traces(line=dict(width=2.5), marker=dict(size=7))
st.plotly_chart(fig_trend, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

# ── Transactions table ────────────────────────────────────────────────────────
txns = q(f"""
    SELECT
        STRFTIME(t.date_id,'%d %b %Y') AS Date,
        t.description                   AS Description,
        c.category_name                 AS Category,
        t.transaction_type              AS Type,
        t.amount                        AS Amount
    FROM fact_transactions t
    JOIN dim_category c ON t.category_id = c.category_id
    WHERE {base_f} ORDER BY t.date_id DESC
""")

st.markdown('<div class="chart-box">', unsafe_allow_html=True)
col_title, col_cat_f, col_type_f = st.columns([3, 1, 1])
col_title.markdown('<div class="chart-title">🧾 All Transactions</div>', unsafe_allow_html=True)
cat_f  = col_cat_f.selectbox("Cat",  ["All"] + all_cats, label_visibility="collapsed")
type_f = col_type_f.selectbox("Type",["All","Income","Expenses"], label_visibility="collapsed")

display = txns.copy()
if cat_f  != "All":      display = display[display["Category"] == cat_f]
if type_f == "Income":   display = display[display["Type"] == "credit"]
if type_f == "Expenses": display = display[display["Type"] == "debit"]

def fmt(row):
    return f"+₹{row['Amount']:,.0f}" if row["Type"] == "credit" else f"-₹{row['Amount']:,.0f}"

display["Amount"] = display.apply(fmt, axis=1)
display = display.drop(columns=["Type"])

st.dataframe(display, use_container_width=True, hide_index=True, height=400,
    column_config={
        "Date":        st.column_config.TextColumn("Date",        width="small"),
        "Description": st.column_config.TextColumn("Description", width="large"),
        "Category":    st.column_config.TextColumn("Category",    width="medium"),
        "Amount":      st.column_config.TextColumn("Amount",      width="small"),
    })
st.markdown("</div>", unsafe_allow_html=True)

st.caption("Powered by DuckDB · Streamlit · Plotly")
