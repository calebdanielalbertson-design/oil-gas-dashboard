import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os
import geopandas as gpd
import folium
from streamlit_folium import st_folium

# Database Path
DB_NAME = "production.db"
MAPS_FOLDER = "maps"

# Page Config
st.set_page_config(page_title="Production Dashboard", layout="wide")

st.title("🛢️ Oil & Gas Production Dashboard")

# --- Navigation ---
page = st.sidebar.radio("Navigation", ["Production Analysis", "Map Explorer"])

if page == "Production Analysis":
    # --- Data Loading ---
    @st.cache_data
    def load_metadata():
        """Load minimal metadata for filters (dates, pools, statuses)."""
        conn = sqlite3.connect(DB_NAME)
        
        # 1. Get Date Range
        try:
            dates = pd.read_sql("SELECT MIN(date) as min_date, MAX(date) as max_date FROM production_data", conn)
            min_date = pd.to_datetime(dates['min_date'][0])
            max_date = pd.to_datetime(dates['max_date'][0])
            
            # 2. Get Pools
            pools = pd.read_sql("SELECT DISTINCT pool FROM production_data ORDER BY pool", conn)
            pool_list = pools['pool'].tolist()

            # 3. Get Statuses
            statuses = pd.read_sql("SELECT DISTINCT status FROM production_data ORDER BY status", conn)
            status_list = statuses['status'].tolist()
        except Exception as e:
                st.error(f"Error reading database metadata: {e}")
                return None, None, [], []
        finally:
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
    min_date, max_date, pool_options, status_options = load_metadata()
    
    if min_date:
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

elif page == "Map Explorer":
    st.header("🗺️ Geospatial Explorer")
    
    # Ensure maps directory exists
    if not os.path.exists(MAPS_FOLDER):
        os.makedirs(MAPS_FOLDER)
        st.warning(f"Created '{MAPS_FOLDER}' folder. Please place your .gpkg or .shp files there.")
    
    # List available map files
    map_files = [f for f in os.listdir(MAPS_FOLDER) if f.endswith(('.gpkg', '.shp', '.geojson'))]
    
    if not map_files:
        st.info("No map files found.")
        st.markdown(f"**Action Required**: Drop your `.gpkg` or `.shp` files into the `{MAPS_FOLDER}` folder in your project directory.")
    else:
        selected_map = st.selectbox("Select a Map Layer", map_files)
        
        if selected_map:
            file_path = os.path.join(MAPS_FOLDER, selected_map)
            
            with st.spinner(f"Loading {selected_map}..."):
                try:
                    # Load data
                    gdf = gpd.read_file(file_path)
                    
                    # Project to lat/lon for Folium (EPSG:4326)
                    if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                         gdf = gdf.to_crs(epsg=4326)

                    # Inspect columns to find numeric candidates for coloring
                    numeric_cols = gdf.select_dtypes(include=['number']).columns.tolist()
                    
                    col_opts, _ = st.columns([1, 2])
                    color_col = col_opts.selectbox("Color by (Column)", numeric_cols) if numeric_cols else None
                    
                    # Create Map tailored to Data Bounds
                    bounds = gdf.total_bounds # [minx, miny, maxx, maxy]
                    center_lat = (bounds[1] + bounds[3]) / 2
                    center_lon = (bounds[0] + bounds[2]) / 2
                    
                    m = folium.Map(location=[center_lat, center_lon], zoom_start=9)
                    
                    # Add Data Layer with Coloring
                    if color_col:
                        folium.Choropleth(
                            geo_data=gdf,
                            data=gdf,
                            columns=[gdf.index, color_col], # Using index to map data to geometry
                            key_on="feature.id",
                            fill_color="YlOrRd",
                            fill_opacity=0.7,
                            line_opacity=0.2,
                            legend_name=color_col
                        ).add_to(m)
                        
                        # Add simple Tooltips
                        folium.GeoJson(
                            gdf,
                            tooltip=folium.GeoJsonTooltip(fields=[color_col], aliases=[color_col])
                        ).add_to(m)
                    else:
                        folium.GeoJson(gdf).add_to(m)

                    # Display Map
                    st_folium(m, width="100%", height=600)
                    
                    # Show Raw Data
                    with st.expander("View Raw Data"):
                        st.dataframe(gdf.drop(columns='geometry').head(100))

                except Exception as e:
                    st.error(f"Error loading map: {e}")
