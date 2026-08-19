import time
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


# ============================================================
# CONFIGURATION
# ============================================================

PARQUET_ROOT = Path("data/parquet")

PARQUET_PATTERN = "data/parquet/**/*.parquet"

REFRESH_SECONDS = 5


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Real-Time E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# LOAD DATA
# ============================================================

def parquet_data_exists():

    return any(
        PARQUET_ROOT.rglob("*.parquet")
    )


def load_data():

    connection = duckdb.connect()

    try:

        df = connection.execute(
            f"""
            SELECT *
            FROM read_parquet(
                '{PARQUET_PATTERN}',
                union_by_name = true
            )
            """
        ).df()

    finally:

        connection.close()

    return df


# ============================================================
# HEADER
# ============================================================

st.title(
    "⚡ Real-Time E-Commerce Analytics"
)

st.caption(
    "Kafka → Validation → Parquet → DuckDB → Streamlit"
)


# ============================================================
# CHECK DATA
# ============================================================

if not parquet_data_exists():

    st.warning(
        "No streaming data available yet."
    )

    st.info(
        "Start Kafka, the consumer and producer "
        "to generate events."
    )

    st.stop()


# ============================================================
# LOAD CURRENT DATA
# ============================================================

try:

    df = load_data()

except Exception as error:

    st.error(
        f"Unable to load streaming data: {error}"
    )

    st.stop()


if df.empty:

    st.warning(
        "No events available."
    )

    st.stop()


# ============================================================
# DATA CLEANING
# ============================================================

df["total_amount"] = pd.to_numeric(
    df["total_amount"],
    errors="coerce",
)

df["quantity"] = pd.to_numeric(
    df["quantity"],
    errors="coerce",
)

df["event_timestamp"] = pd.to_datetime(
    df["event_timestamp"],
    errors="coerce",
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title(
    "Dashboard Controls"
)

auto_refresh = st.sidebar.checkbox(
    "Auto Refresh",
    value=True,
)

if st.sidebar.button(
    "Refresh Now"
):
    st.rerun()

st.sidebar.write(
    f"Refresh interval: "
    f"{REFRESH_SECONDS} seconds"
)

st.sidebar.divider()

st.sidebar.write(
    f"Loaded Events: {len(df):,}"
)


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_orders = len(df)

total_items = int(
    df["quantity"].sum()
)

total_revenue = float(
    df["total_amount"].sum()
)

average_order_value = (
    total_revenue / total_orders
    if total_orders > 0
    else 0
)


# ============================================================
# KPI CARDS
# ============================================================

st.subheader(
    "📌 Live Business KPIs"
)

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "Total Orders",
        f"{total_orders:,}",
    )

with col2:

    st.metric(
        "Items Sold",
        f"{total_items:,}",
    )

with col3:

    st.metric(
        "Total Revenue",
        f"₹{total_revenue:,.0f}",
    )

with col4:

    st.metric(
        "Average Order Value",
        f"₹{average_order_value:,.0f}",
    )


st.divider()


# ============================================================
# CATEGORY ANALYTICS
# ============================================================

st.subheader(
    "📦 Category Performance"
)

category_data = (
    df.groupby(
        "category",
        as_index=False,
    )
    .agg(
        revenue=(
            "total_amount",
            "sum",
        ),
        orders=(
            "order_id",
            "count",
        ),
    )
    .sort_values(
        "revenue",
        ascending=False,
    )
)

chart_col1, chart_col2 = (
    st.columns(2)
)

with chart_col1:

    st.write(
        "Revenue by Category"
    )

    st.bar_chart(
        category_data.set_index(
            "category"
        )["revenue"]
    )


# ============================================================
# PAYMENT ANALYTICS
# ============================================================

payment_data = (
    df.groupby(
        "payment_method",
        as_index=False,
    )
    .agg(
        orders=(
            "order_id",
            "count",
        )
    )
    .sort_values(
        "orders",
        ascending=False,
    )
)

with chart_col2:

    st.write(
        "Orders by Payment Method"
    )

    st.bar_chart(
        payment_data.set_index(
            "payment_method"
        )["orders"]
    )


st.divider()


# ============================================================
# PRODUCT ANALYTICS
# ============================================================

st.subheader(
    "🏆 Top Products"
)

product_data = (
    df.groupby(
        "product_name",
        as_index=False,
    )
    .agg(
        revenue=(
            "total_amount",
            "sum",
        ),
        units_sold=(
            "quantity",
            "sum",
        ),
        orders=(
            "order_id",
            "count",
        ),
    )
    .sort_values(
        "revenue",
        ascending=False,
    )
)

product_col1, product_col2 = (
    st.columns(2)
)

with product_col1:

    st.write(
        "Top Products by Revenue"
    )

    st.bar_chart(
        product_data.head(10).set_index(
            "product_name"
        )["revenue"]
    )


with product_col2:

    st.write(
        "Product Performance"
    )

    st.dataframe(
        product_data.head(10),
        use_container_width=True,
        hide_index=True,
    )


st.divider()


# ============================================================
# ORDER STATUS
# ============================================================

st.subheader(
    "🚚 Order Status"
)

status_data = (
    df.groupby(
        "order_status",
        as_index=False,
    )
    .agg(
        orders=(
            "order_id",
            "count",
        )
    )
    .sort_values(
        "orders",
        ascending=False,
    )
)

st.bar_chart(
    status_data.set_index(
        "order_status"
    )["orders"]
)


st.divider()


# ============================================================
# GEOGRAPHIC ANALYTICS
# ============================================================

st.subheader(
    "🌍 Top Locations"
)

location_data = (
    df.groupby(
        [
            "city",
            "country",
        ],
        as_index=False,
    )
    .agg(
        orders=(
            "order_id",
            "count",
        ),
        revenue=(
            "total_amount",
            "sum",
        ),
    )
    .sort_values(
        "revenue",
        ascending=False,
    )
)

st.dataframe(
    location_data.head(10),
    use_container_width=True,
    hide_index=True,
)


st.divider()


# ============================================================
# RECENT EVENTS
# ============================================================

st.subheader(
    "🟢 Recent Streaming Events"
)

recent_columns = [
    "event_timestamp",
    "order_id",
    "product_name",
    "category",
    "quantity",
    "total_amount",
    "payment_method",
    "order_status",
]

recent_events = (
    df.sort_values(
        "event_timestamp",
        ascending=False,
    )
    [recent_columns]
    .head(10)
)

st.dataframe(
    recent_events,
    use_container_width=True,
    hide_index=True,
)


# ============================================================
# PIPELINE STATUS
# ============================================================

st.divider()

st.subheader(
    "⚙️ Pipeline Status"
)

status1, status2, status3 = (
    st.columns(3)
)

with status1:

    st.success(
        "Parquet Storage: ACTIVE"
    )

with status2:

    st.success(
        "DuckDB Analytics: ACTIVE"
    )

with status3:

    st.success(
        "Dashboard: ACTIVE"
    )


# ============================================================
# AUTO REFRESH
# ============================================================

if auto_refresh:

    time.sleep(
        REFRESH_SECONDS
    )

    st.rerun()