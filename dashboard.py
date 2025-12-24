import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Database Path
DB_NAME = "production.db"

# Page Config
st.set_page_config(page_title="Production Dashboard", layout="wide")

st.title("🛢️ Oil & Gas Production Dashboard")

# --- Data Loading ---
@st.cache_data
def load_data():
    """Load minimal data necessary for filters and main charts."""
    conn = sqlite3.connect(DB_NAME)
    
    # 1. Get Date Range
    dates = pd.read_sql("SELECT MIN(date) as min_date, MAX(date) as max_date FROM production_data", conn)
    min_date = pd.to_datetime(dates['min_date'][0])
    max_date = pd.to_datetime(dates['max_date'][0])
    
    # 2. Get Pools
    pools = pd.read_sql("SELECT DISTINCT pool FROM production_data ORDER BY pool", conn)
    pool_list = pools['pool'].tolist()
    
    conn.close()
    return min_date, max_date, pool_list

@st.cache_data
def get_chart_data(start_date, end_date, selected_pools):
    conn = sqlite3.connect(DB_NAME)
    
    # Base Query Construction
    params = [start_date, end_date]
    pool_clause = ""
    if selected_pools:
        placeholders = ",".join("?" * len(selected_pools))
        pool_clause = f"AND pool IN ({placeholders})"
        params.extend(selected_pools)
    
    # Aggregation Query for Stacked Area Chart
    # Group by Month and Status
    query = f"""
    SELECT 
        strftime('%Y-%m', date) as month,
        status,
        COUNT(*) as well_count,
        SUM(bbls_oil) as total_oil
    FROM production_data
    WHERE date >= ? AND date <= ?
    {pool_clause}
    GROUP BY 1, 2
    ORDER BY 1
    """
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    return df

# Initialize Data
try:
    min_date, max_date, pool_options = load_data()
except Exception as e:
    st.error(f"Error loading database: {e}")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Date Filter
start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.sidebar.error("Start date must be before end date.")

# Pool Filter
selected_pools = st.sidebar.multiselect("Select Pool(s)", pool_options, default=pool_options[:1] if pool_options else None)

# --- Main Content ---

# Fetch Filtered Data
if st.button("Update Analysis"):
    with st.spinner("Querying database..."):
        df_chart = get_chart_data(start_date, end_date, selected_pools)

        # --- Stacked Area Chart (Well Status) ---
        st.subheader("Well Status Over Time")
        
        # Color Palette Definition
        # Active (A): Green
        # Abandoned (AB): Red
        # Inactive (IA): Light Red (Salmon)
        # Inactive 1 (IA 1 - A): Lighter Red/Orange
        # Inactive 2 (IA 2 - A): Pinkish
        color_map = {
            "A": "#28a745",        # Green
            "AB": "#dc3545",       # Red
            "IA": "#f88379",       # Light Red / Coral
            "IA 1 - A": "#ffc107", # Amber/Orange (Warning)
            "IA 2 - A": "#e83e8c", # Pink
            "Unknown": "#6c757d"   # Grey
        }

        # Sorting status to ensure consistent stacking order (Active usually at bottom or top?)
        # Let's stack such that Active is foundation? Or AB on top? 
        # Plotly handles this automatically, but custom order helps.
        category_orders = {"status": ["A", "IA 1 - A", "IA 2 - A", "IA", "AB"]}

        fig = px.area(
            df_chart, 
            x="month", 
            y="well_count", 
            color="status",
            color_discrete_map=color_map,
            category_orders=category_orders,
            title="Well Count by Status (Stacked)",
            labels={"well_count": "Number of Wells", "month": "Date", "status": "Status"}
        )
        
        st.plotly_chart(fig, use_container_width=True)

        # --- Metrics Row ---
        col1, col2, col3 = st.columns(3)
        total_oil = df_chart['total_oil'].sum()
        avg_wells = df_chart.groupby('month')['well_count'].sum().mean()
        
        col1.metric("Total Oil Produced", f"{total_oil:,.0f} bbls")
        col2.metric("Avg Active Wells", f"{avg_wells:,.0f}")
        col3.metric("Data Points", f"{len(df_chart)}")

        # --- Area Chart for Production ---
        # User requested coloring different categories for a stacked area chart.
        # Maybe they want Production by Status too?
        st.subheader("Oil Production by Status")
        fig2 = px.area(
            df_chart, 
            x="month", 
            y="total_oil", 
            color="status",
            color_discrete_map=color_map,
            category_orders=category_orders,
            title="Oil Production by Status (Stacked)",
             labels={"total_oil": "Oil Production (bbls)", "month": "Date", "status": "Status"}
        )
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("Select filters and click 'Update Analysis' to view data.")
