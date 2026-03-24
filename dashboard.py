import streamlit as st
import redis
import json
import pandas as pd
import matplotlib.pyplot as plt

# Page config
st.set_page_config(page_title="Sales Analytics Dashboard", layout="wide")
st.title("Sales Analytics Dashboard")
st.caption("Kafka → Redis → Spark ETL → ML Pipeline")

# Connects to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Loads data from Redis
raw_processed   = r.get("sales_processed")
raw_summary     = r.get("sales_summary")
raw_predictions = r.get("sales_predictions")

if not raw_processed:
    st.error("No data found in Redis. Please run producer.py and spark_etl.py first.")
    st.stop()

df         = pd.DataFrame(json.loads(raw_processed))
summary_df = pd.DataFrame(json.loads(raw_summary))

# KPI cards 
st.subheader("Overview")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders",    len(df))
col2.metric("Total Revenue",   f"€{df['revenue'].sum():,.2f}")
col3.metric("Avg Order Value", f"€{df['revenue'].mean():,.2f}")
col4.metric("Products Sold",   df["Product"].nunique())

st.divider()

# Revenue per salesman bar chart
st.subheader("Revenue per Salesman")
col_a, col_b = st.columns(2)

with col_a:
    fig1, ax1 = plt.subplots(figsize=(6, 4))
    summary_df_sorted = summary_df.sort_values("total_revenue", ascending=False)
    ax1.bar(summary_df_sorted["SalesMan"], summary_df_sorted["total_revenue"],
            color=["#4472C4", "#ED7D31", "#A9D18E", "#FF0000", "#FFC000"])
    ax1.set_xlabel("Salesman")
    ax1.set_ylabel("Total Revenue (€)")
    ax1.set_title("Total Revenue by Salesman")
    plt.tight_layout()
    st.pyplot(fig1)

with col_b:
    fig2, ax2 = plt.subplots(figsize=(6, 4))
    product_rev = df.groupby("Product")["revenue"].sum().sort_values(ascending=False)
    ax2.bar(product_rev.index, product_rev.values, color="#4472C4")
    ax2.set_xlabel("Product")
    ax2.set_ylabel("Total Revenue (€)")
    ax2.set_title("Total Revenue by Product")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    st.pyplot(fig2)

st.divider()

# Salesman summary table
st.subheader("Salesman Summary Table")
st.dataframe(summary_df.sort_values("total_revenue", ascending=False), use_container_width=True)

st.divider()

# ML Predictions
st.subheader("ML Model: Predicted vs Actual Revenue")

if raw_predictions:
    pred_df = pd.read_json(raw_predictions)

    fig3, ax3 = plt.subplots(figsize=(10, 4))
    ax3.plot(pred_df["order_id"], pred_df["revenue"],
             label="Actual", color="#4472C4", linewidth=1)
    ax3.plot(pred_df["order_id"], pred_df["predicted_revenue"],
             label="Predicted", color="#ED7D31", linewidth=1, linestyle="--")
    ax3.set_xlabel("Order ID")
    ax3.set_ylabel("Revenue (€)")
    ax3.set_title("Actual vs Predicted Revenue (Linear Regression)")
    ax3.legend()
    plt.tight_layout()
    st.pyplot(fig3)
else:
    st.info("Run ml_model.py to see predictions here.")

st.divider()

# Raw data explorer
st.subheader("Raw Orders Explorer")
salesman_filter = st.multiselect("Filter by Salesman",
                                  options=df["SalesMan"].unique().tolist(),
                                  default=df["SalesMan"].unique().tolist())
filtered = df[df["SalesMan"].isin(salesman_filter)]
st.dataframe(filtered, use_container_width=True)