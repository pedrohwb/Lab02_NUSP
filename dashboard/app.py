import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

load_dotenv()

st.set_page_config(
    page_title="Online Retail II — Lab02 Dashboard",
    page_icon="🛍️",
    layout="wide",
)


@st.cache_resource(show_spinner="Conectando ao banco de dados…")
def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "lab02_retail")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


@st.cache_data(ttl=300, show_spinner="Carregando dados…")
def run_query(sql: str) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn)
    except OperationalError as exc:
        st.error(
            f"⚠️ Não foi possível conectar ao PostgreSQL.\n\n"
            f"Verifique se o banco está rodando e as variáveis de ambiente estão corretas.\n\n"
            f"Detalhe: {exc}"
        )
        st.stop()
    except ProgrammingError as exc:
        st.error(
            "⚠️ A camada Gold ainda não está pronta.\n\n"
            "Aguarde o DBT finalizar e recarregue a página.\n\n"
            f"Detalhe: {exc}"
        )
        st.stop()


REQUIRED_GOLD_TABLES = ("dim_product", "dim_customer", "dim_country", "fct_sales")


@st.cache_data(ttl=30, show_spinner="Verificando camada Gold…")
def gold_is_ready() -> tuple[bool, list[str]]:
    checks = ",\n    ".join(
        f"to_regclass('gold.{t}') IS NOT NULL AS {t}" for t in REQUIRED_GOLD_TABLES
    )
    try:
        with get_engine().connect() as conn:
            row = conn.execute(text(f"SELECT\n    {checks};")).mappings().one()
    except OperationalError:
        return False, list(REQUIRED_GOLD_TABLES)
    missing = [t for t in REQUIRED_GOLD_TABLES if not row[t]]
    return len(missing) == 0, missing


# ── SQL Queries (all reading from gold schema) ────────────────────────────────

SQL_MONTHLY_REVENUE = """
SELECT
    f.period,
    ROUND(SUM(f.gross_revenue)::numeric, 2) AS monthly_revenue
FROM gold.fct_sales f
WHERE f.is_canceled = FALSE
GROUP BY f.period
ORDER BY f.period;
"""

SQL_TOP_COUNTRIES = """
SELECT
    c.country_name,
    ROUND(SUM(f.gross_revenue)::numeric, 2) AS total_revenue
FROM gold.fct_sales f
JOIN gold.dim_country c ON f.country_id = c.country_id
WHERE f.is_canceled = FALSE
GROUP BY c.country_name
ORDER BY total_revenue DESC
LIMIT 10;
"""

SQL_TOP_PRODUCTS = """
SELECT
    p.stock_code,
    p.description,
    SUM(f.quantity) AS total_quantity
FROM gold.fct_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
WHERE f.is_canceled = FALSE
GROUP BY p.stock_code, p.description
ORDER BY total_quantity DESC
LIMIT 10;
"""

SQL_CANCELLATION = """
SELECT
    CASE WHEN is_canceled THEN 'Cancelado' ELSE 'Concluído' END AS status,
    COUNT(*) AS total_rows,
    ROUND(SUM(gross_revenue)::numeric, 2) AS total_revenue
FROM gold.fct_sales
GROUP BY is_canceled;
"""

SQL_MONTHLY_ORDERS = """
SELECT
    f.period,
    COUNT(DISTINCT f.invoice) AS unique_orders
FROM gold.fct_sales f
WHERE f.is_canceled = FALSE
GROUP BY f.period
ORDER BY f.period;
"""

SQL_TOP_CUSTOMERS = """
SELECT
    cu.customer_id,
    ROUND(SUM(f.gross_revenue)::numeric, 2) AS total_revenue
FROM gold.fct_sales f
JOIN gold.dim_customer cu ON f.customer_key = cu.customer_key
WHERE f.is_canceled = FALSE
  AND cu.customer_id IS NOT NULL
GROUP BY cu.customer_id
ORDER BY total_revenue DESC
LIMIT 10;
"""

SQL_SUMMARY = """
SELECT
    (SELECT COUNT(*) FROM gold.dim_product)   AS products,
    (SELECT COUNT(*) FROM gold.dim_customer)  AS customers,
    (SELECT COUNT(*) FROM gold.dim_country)   AS countries,
    (SELECT COUNT(*) FROM gold.fct_sales)     AS transactions,
    (SELECT ROUND(SUM(gross_revenue)::numeric, 2)
       FROM gold.fct_sales WHERE is_canceled = FALSE) AS total_revenue;
"""

# ── Dashboard Layout ──────────────────────────────────────────────────────────

st.title("🛍️ Online Retail II — Lab02 Dashboard (camada Gold)")
st.markdown(
    "Dashboard conectado à camada **Gold** gerada pelo **DBT** no schema `gold`. "
    "Pipeline: Raw → Silver (Python) → Gold (DBT) → Dashboard (Streamlit)."
)
st.divider()

ready, missing = gold_is_ready()
if not ready:
    st.warning(
        "A camada Gold ainda não foi materializada. "
        "Aguarde o serviço `dbt` finalizar e recarregue a página."
    )
    st.code("\n".join(f"gold.{t}" for t in missing))
    if st.button("Atualizar status"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

summary = run_query(SQL_SUMMARY).iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Produtos únicos", f"{int(summary['products']):,}")
col2.metric("Clientes únicos", f"{int(summary['customers']):,}")
col3.metric("Países", f"{int(summary['countries']):,}")
col4.metric("Transações", f"{int(summary['transactions']):,}")
col5.metric("Receita total (£)", f"£ {float(summary['total_revenue']):,.2f}")

st.divider()

# ── Chart 1: Monthly Revenue ──────────────────────────────────────────────────
st.subheader("1. 📈 Receita Mensal")
df_monthly = run_query(SQL_MONTHLY_REVENUE)
fig1 = px.line(
    df_monthly,
    x="period",
    y="monthly_revenue",
    markers=True,
    title="Receita Bruta Mensal (excl. cancelamentos)",
    labels={"period": "Período", "monthly_revenue": "Receita (£)"},
)
fig1.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Top 10 Countries ─────────────────────────────────────────────────
st.subheader("2. 🌍 Top 10 Países por Receita")
df_countries = run_query(SQL_TOP_COUNTRIES)
fig2 = px.bar(
    df_countries,
    x="country_name",
    y="total_revenue",
    color="total_revenue",
    color_continuous_scale="Blues",
    title="Top 10 Países por Receita Total",
    labels={"country_name": "País", "total_revenue": "Receita (£)"},
)
fig2.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
st.plotly_chart(fig2, use_container_width=True)

col_left, col_right = st.columns(2)

# ── Chart 3: Top 10 Products ──────────────────────────────────────────────────
with col_left:
    st.subheader("3. 📦 Top 10 Produtos por Quantidade")
    df_products = run_query(SQL_TOP_PRODUCTS)
    df_products["label"] = df_products["stock_code"] + " – " + df_products["description"].str[:30]
    fig3 = px.bar(
        df_products.sort_values("total_quantity"),
        x="total_quantity",
        y="label",
        orientation="h",
        title="Top 10 Produtos (unidades vendidas)",
        labels={"total_quantity": "Quantidade", "label": "Produto"},
        color="total_quantity",
        color_continuous_scale="Greens",
    )
    fig3.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

# ── Chart 4: Cancellation Analysis ───────────────────────────────────────────
with col_right:
    st.subheader("4. ❌ Análise de Cancelamentos")
    df_cancel = run_query(SQL_CANCELLATION)
    fig4 = px.pie(
        df_cancel,
        names="status",
        values="total_rows",
        title="Transações: Concluídas vs Canceladas",
        color_discrete_sequence=["#2ecc71", "#e74c3c"],
        hole=0.4,
    )
    st.plotly_chart(fig4, use_container_width=True)

# ── Chart 5: Monthly Orders ───────────────────────────────────────────────────
st.subheader("5. 🗓️ Pedidos Únicos por Mês")
df_orders = run_query(SQL_MONTHLY_ORDERS)
fig5 = px.area(
    df_orders,
    x="period",
    y="unique_orders",
    title="Número de Pedidos Únicos por Mês",
    labels={"period": "Período", "unique_orders": "Pedidos"},
    color_discrete_sequence=["#3498db"],
)
fig5.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig5, use_container_width=True)

# ── Chart 6: Top 10 Customers ─────────────────────────────────────────────────
st.subheader("6. 👤 Top 10 Clientes por Receita")
df_customers = run_query(SQL_TOP_CUSTOMERS)
fig6 = px.bar(
    df_customers,
    x="customer_id",
    y="total_revenue",
    title="Top 10 Clientes por Receita Total",
    labels={"customer_id": "ID do Cliente", "total_revenue": "Receita (£)"},
    color="total_revenue",
    color_continuous_scale="Oranges",
)
fig6.update_layout(coloraxis_showscale=False)
st.plotly_chart(fig6, use_container_width=True)

st.divider()
st.caption(
    "Fonte: Online Retail II dataset — camada Gold gerada pelo DBT · Lab02 NUSP"
)
