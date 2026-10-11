"""Streamlit dashboard backed by read-only Amazon Athena queries."""

from __future__ import annotations

import os
import re
import time

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

st.set_page_config(page_title="Cloud Event Analytics", page_icon="📊", layout="wide")
st.title("Cloud Event Analytics")
st.caption("Synthetic commerce events · Amazon S3 + AWS Glue + Amazon Athena")

region = os.getenv("AWS_REGION", "ap-south-1")
database = os.getenv("ATHENA_DATABASE", "event_analytics")
table = os.getenv("ATHENA_TABLE", "events_curated")
workgroup = os.getenv("ATHENA_WORKGROUP", "cloud-event-analytics-workgroup")
output = os.getenv("ATHENA_OUTPUT", "")
days_back = st.sidebar.slider("Analysis window (days)", min_value=7, max_value=90, value=30)

if not re.fullmatch(r"[A-Za-z0-9_]+", database) or not re.fullmatch(r"[A-Za-z0-9_]+", table):
    st.error("ATHENA_DATABASE and ATHENA_TABLE may contain only letters, digits, and underscores.")
    st.stop()


@st.cache_data(ttl=300, show_spinner=False)
def query_athena(sql: str, region_name: str, catalog_database: str, query_workgroup: str, result_uri: str) -> pd.DataFrame:
    """Submit a bounded read query, wait for completion, and fetch paginated rows."""
    client = boto3.client("athena", region_name=region_name)
    request: dict[str, object] = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": catalog_database},
        "WorkGroup": query_workgroup,
    }
    if result_uri:
        request["ResultConfiguration"] = {"OutputLocation": result_uri}
    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        if status == "SUCCEEDED":
            break
        if status in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", "Unknown Athena error")
            raise RuntimeError(reason)
        time.sleep(1)
    else:
        client.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError("Athena query exceeded the 120-second dashboard limit.")

    paginator = client.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=query_id)
    columns: list[str] = []
    records: list[list[str | None]] = []
    first_page = True
    for page in pages:
        rows = page["ResultSet"]["Rows"]
        if first_page and rows:
            columns = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]
            rows = rows[1:]
            first_page = False
        for row in rows:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            records.append(values + [None] * (len(columns) - len(values)))
    return pd.DataFrame(records, columns=columns)


if st.sidebar.button("Refresh Athena results"):
    query_athena.clear()

if not output:
    st.info("Set ATHENA_OUTPUT to the S3 results URI from Terraform before querying Athena.")
    st.code("terraform -chdir=infra output -raw athena_output_uri")

cutoff = f"date_format(date_add('day', -{days_back}, current_date), '%Y-%m-%d')"
table_ref = f"{database}.{table}"

queries = {
    "daily": f"""
        SELECT event_date AS day,
               ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd,
               COUNT_IF(event_type = 'purchase') AS orders
        FROM {table_ref}
        WHERE event_date >= {cutoff}
        GROUP BY event_date
        ORDER BY event_date
    """,
    "funnel": f"""
        WITH session_signals AS (
            SELECT session_id,
                   MAX(IF(event_type = 'product_view', 1, 0)) AS viewed,
                   MAX(IF(event_type = 'add_to_cart', 1, 0)) AS carted,
                   MAX(IF(event_type = 'checkout', 1, 0)) AS checked_out,
                   MAX(IF(event_type = 'purchase', 1, 0)) AS purchased
            FROM {table_ref}
            WHERE event_date >= {cutoff}
            GROUP BY session_id
        )
        SELECT 1 AS stage_order, 'product_view' AS event_type, COUNT_IF(viewed = 1) AS sessions FROM session_signals
        UNION ALL
        SELECT 2, 'add_to_cart', COUNT_IF(carted = 1) FROM session_signals
        UNION ALL
        SELECT 3, 'checkout', COUNT_IF(checked_out = 1) FROM session_signals
        UNION ALL
        SELECT 4, 'purchase', COUNT_IF(purchased = 1) FROM session_signals
        ORDER BY stage_order
    """,
    "regions": f"""
        SELECT region,
               ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd,
               COUNT_IF(event_type = 'purchase') AS orders,
               COUNT(DISTINCT IF(event_type = 'purchase', session_id, NULL)) AS buying_sessions
        FROM {table_ref}
        WHERE event_date >= {cutoff}
        GROUP BY region
        ORDER BY revenue_usd DESC
    """,
    "categories": f"""
        SELECT category,
               ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd,
               COUNT_IF(event_type = 'purchase') AS orders
        FROM {table_ref}
        WHERE event_date >= {cutoff} AND event_type = 'purchase'
        GROUP BY category
        ORDER BY revenue_usd DESC
        LIMIT 10
    """,
}

try:
    if not output:
        st.stop()
    with st.spinner("Running partition-filtered analytics in Athena…"):
        daily = query_athena(queries["daily"], region, database, workgroup, output)
        funnel = query_athena(queries["funnel"], region, database, workgroup, output)
        regions = query_athena(queries["regions"], region, database, workgroup, output)
        categories = query_athena(queries["categories"], region, database, workgroup, output)
    funnel = funnel.drop(columns=["stage_order"], errors="ignore")

    for frame, numeric_columns in (
        (daily, ["revenue_usd", "orders"]),
        (funnel, ["sessions"]),
        (regions, ["revenue_usd", "orders", "buying_sessions"]),
        (categories, ["revenue_usd", "orders"]),
    ):
        for column in numeric_columns:
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    event_sessions = dict(zip(funnel.get("event_type", []), funnel.get("sessions", []), strict=False))
    views = int(event_sessions.get("product_view", 0))
    purchases = int(event_sessions.get("purchase", 0))
    total_revenue = float(daily.get("revenue_usd", pd.Series(dtype=float)).sum())
    total_orders = int(daily.get("orders", pd.Series(dtype=float)).sum())

    first, second, third = st.columns(3)
    first.metric("Revenue", f"${total_revenue:,.2f}", f"Last {days_back} days")
    second.metric("Purchase events", f"{total_orders:,}")
    third.metric("View-to-purchase sessions", f"{(purchases / views * 100):.1f}%" if views else "0.0%")

    left, right = st.columns(2)
    with left:
        st.subheader("Daily revenue")
        if daily.empty:
            st.info("No curated events found. Upload a sample and run the Glue pipeline first.")
        else:
            daily["day"] = pd.to_datetime(daily["day"], errors="coerce")
            st.plotly_chart(px.area(daily, x="day", y="revenue_usd", markers=True), use_container_width=True)
    with right:
        st.subheader("Session funnel")
        st.plotly_chart(px.funnel(funnel, x="sessions", y="event_type"), use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by AWS region")
        st.plotly_chart(px.bar(regions, x="region", y="revenue_usd", color="region"), use_container_width=True)
    with right:
        st.subheader("Top categories")
        st.plotly_chart(px.bar(categories, x="revenue_usd", y="category", orientation="h"), use_container_width=True)

    with st.expander("Data notes and query cost controls"):
        st.write(
            "All events are fictional and contain no real customer data. Queries filter the "
            "string partition key `event_date`; the selected window is capped at 90 days. "
            "Athena charges by data scanned, so keep the date filter and avoid SELECT * on large tables."
        )
except (BotoCoreError, ClientError, NoCredentialsError, RuntimeError, TimeoutError) as error:
    st.error(f"Could not load analytics from Athena: {error}")
    st.caption("Confirm AWS credentials, region, workgroup, table partitions, and Athena S3 result access.")
