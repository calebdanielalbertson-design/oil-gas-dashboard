import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px

# Database Path
DB_NAME = "production.db"

# Page Config
st.set_page_config(page_title="Production Dashboard", layout="wide")

st.title("🛢️ Oil & Gas Production Dashboard")

# --- Data Loading ---
@st.cache_data
def load_metadata():
    """Load minimal metadata for filters (dates, pools, statuses)."""
    conn = sqlite3.connect(DB_NAME)
    
    # 1. Get Date Range
    dates = pd.read_sql("SELECT MIN(date) as min_date, MAX(date) as max_date FROM production_data", conn)
    min_date = pd.to_datetime(dates['min_date'][0])
    max_date = pd.to_datetime(dates['max_date'][0])
    
    # 2. Get Pools
    pools = pd.read_sql("SELECT DISTINCT pool FROM production_data ORDER BY pool", conn)
    pool_list = pools['pool'].tolist()

    # 3. Get Statuses
    statuses = pd.read_sql("SELECT DISTINCT status FROM production_data ORDER BY status", conn)
    status_list = statuses['status'].tolist()
    
    conn.close()
    return min_date, max_date, pool_list, status_list

def get_chart_data(start_date, end_date, selected_pools, selected_statuses):
    conn = sqlite3.connect(DB_NAME)
    
    # Construct Query params
    params = [start_date, end_date]
    
    # Pool Clause
    pool_clause = ""
    if selected_pools:
        placeholders = ",".join("?" * len(selected_pools))
        pool_clause = f"AND pool IN ({placeholders})"
        params.extend(selected_pools)
        
    # Status Clause
    status_clause = ""
    if selected_statuses:
        placeholders = ",".join("?" * len(selected_statuses))
        status_clause = f"AND status IN ({placeholders})"
        params.extend(selected_statuses)

    # Aggregation Query
    query = f"""
    SELECT 
        strftime('%Y-%m', date) as month,
        status,
        COUNT(*) as well_count,
        SUM(bbls_oil) as total_oil
    FROM production_data
    WHERE date >= ? AND date <= ?
    {pool_clause}
    {status_clause}
    GROUP BY 1, 2
    ORDER BY 1
    """
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# Initialize Metadata
try:
    min_date, max_date, pool_options, status_options = load_metadata()
except Exception as e:
    st.error(f"Error loading database: {e}")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Chart Type moved to Sidebar for persistence
chart_type = st.sidebar.radio("Chart Type", ["Stacked Area", "Stacked Bar", "Line Chart"])

# Date Filter
start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.sidebar.error("Start date must be before end date.")

# Pool Filter
selected_pools = st.sidebar.multiselect("Select Pool(s)", pool_options, default=pool_options[:1] if pool_options else None)

# Status Filter (New)
selected_statuses = st.sidebar.multiselect("Select Status", status_options, default=status_options)

# Update Button
if st.sidebar.button("Update Analysis"):
    with st.spinner("Querying database..."):
        # Store result in session state
        st.session_state.data = get_chart_data(start_date, end_date, selected_pools, selected_statuses)

# --- Main Content ---

# Check if data exists in session state
if 'data' in st.session_state:
    df_chart = st.session_state.data
    
    if df_chart.empty:
        st.warning("No data found for the selected filters.")
    else:
        # --- Common Chart Settings ---
        color_map = {
            "A": "#28a745",        # Green
            "AB": "#dc3545",       # Red
            "IA": "#f88379",       # Light Red / Coral
            "IA 1 - A": "#ffc107", # Amber/Orange (Warning)
            "IA 2 - A": "#e83e8c", # Pink
            "Unknown": "#6c757d"   # Grey
        }
        category_orders = {"status": ["A", "IA 1 - A", "IA 2 - A", "IA", "AB"]}

        # --- Chart Helper Function ---
        def render_chart(df, y_col, title, y_label):
            if chart_type == "Stacked Bar":
                return px.bar(df, x="month", y=y_col, color="status", 
                              color_discrete_map=color_map, category_orders=category_orders,
                              title=f"{title} (Stacked Bar)", labels={y_col: y_label, "month": "Date"})
            elif chart_type == "Line Chart":
                return px.line(df, x="month", y=y_col, color="status",
                               color_discrete_map=color_map, category_orders=category_orders,
                               title=f"{title} (Line)", labels={y_col: y_label, "month": "Date"})
            else: # Stacked Area
                return px.area(df, x="month", y=y_col, color="status",
                               color_discrete_map=color_map, category_orders=category_orders,
                               title=f"{title} (Stacked Area)", labels={y_col: y_label, "month": "Date"})

        # 1. Well Count Chart
        st.plotly_chart(render_chart(df_chart, "well_count", "Well Count by Status", "Number of Wells"), use_container_width=True)

        # 2. Metrics
        col1, col2, col3 = st.columns(3)
        total_oil = df_chart['total_oil'].sum()
        avg_wells = df_chart.groupby('month')['well_count'].sum().mean()
        
        col1.metric("Total Oil Produced", f"{total_oil:,.0f} bbls")
        col2.metric("Avg Active Wells", f"{avg_wells:,.0f}")
        col3.metric("Data Points", f"{len(df_chart)}")

        # 3. Production Chart
        st.plotly_chart(render_chart(df_chart, "total_oil", "Oil Production by Status", "Oil Production (bbls)"), use_container_width=True)

else:
    st.info("👈 Select filters and click 'Update Analysis' in the sidebar to load data.")
