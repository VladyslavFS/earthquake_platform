# streamlit_app.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional


# Page config
st.set_page_config(
    page_title="🌍 Earthquake Monitor",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)


class DataFetcher:
    """Handles database connections and data fetching"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.config = db_config
        self._conn = None
    
    def connect(self):
        """Establish database connection"""
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(**self.config)
        return self._conn
    
    def fetch_query(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            conn = self.connect()
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"Database error: {str(e)}")
            return pd.DataFrame()
    
    def get_recent_earthquakes(self, days: int = 7, min_magnitude: float = 0.0) -> pd.DataFrame:
        """Fetch recent earthquakes"""
        query = f"""
        SELECT 
            time,
            latitude,
            longitude,
            depth,
            mag,
            place,
            mag_type,
            type
        FROM ods.fct_earthquake
        WHERE time >= NOW() - INTERVAL '{days} days'
            AND mag >= {min_magnitude}
        ORDER BY time DESC
        LIMIT 1000
        """
        return self.fetch_query(query)
    
    def get_daily_stats(self, days: int = 30) -> pd.DataFrame:
        """Fetch daily statistics"""
        query = f"""
        SELECT 
            date,
            value as avg_magnitude
        FROM dm.fct_avg_day_earthquake
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY date
        """
        return self.fetch_query(query)
    
    def get_daily_counts(self, days: int = 30) -> pd.DataFrame:
        """Fetch daily earthquake counts"""
        query = f"""
        SELECT 
            date,
            value as count
        FROM dm.fct_count_day_earthquake
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
        ORDER BY date
        """
        return self.fetch_query(query)
    
    def get_magnitude_distribution(self, days: int = 7) -> pd.DataFrame:
        """Get magnitude distribution"""
        query = f"""
        SELECT 
            FLOOR(mag) as magnitude_range,
            COUNT(*) as count
        FROM ods.fct_earthquake
        WHERE time >= NOW() - INTERVAL '{days} days'
        GROUP BY FLOOR(mag)
        ORDER BY magnitude_range
        """
        return self.fetch_query(query)
    
    def get_top_regions(self, days: int = 7, limit: int = 10) -> pd.DataFrame:
        """Get most active regions"""
        query = f"""
        SELECT 
            place,
            COUNT(*) as count,
            AVG(mag) as avg_magnitude,
            MAX(mag) as max_magnitude
        FROM ods.fct_earthquake
        WHERE time >= NOW() - INTERVAL '{days} days'
            AND place IS NOT NULL
        GROUP BY place
        ORDER BY count DESC
        LIMIT {limit}
        """
        return self.fetch_query(query)
    
    def close(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()


class DashboardVisualizer:
    """Creates visualizations for dashboard"""
    
    @staticmethod
    def create_map(df: pd.DataFrame) -> go.Figure:
        """Create earthquake map"""
        if df.empty:
            return go.Figure()
        
        # Color scale based on magnitude
        fig = px.scatter_geo(
            df,
            lat='latitude',
            lon='longitude',
            size='mag',
            color='mag',
            hover_name='place',
            hover_data={
                'time': True,
                'depth': ':.2f',
                'mag': ':.1f',
                'latitude': False,
                'longitude': False
            },
            color_continuous_scale='Reds',
            size_max=20,
            title='Recent Earthquakes Worldwide'
        )
        
        fig.update_layout(
            geo=dict(
                projection_type='natural earth',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
            ),
            height=600
        )
        
        return fig
    
    @staticmethod
    def create_timeline(df: pd.DataFrame) -> go.Figure:
        """Create timeline of earthquakes"""
        if df.empty:
            return go.Figure()
        
        fig = px.scatter(
            df,
            x='time',
            y='mag',
            size='mag',
            color='mag',
            hover_data=['place', 'depth'],
            title='Earthquake Timeline',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            xaxis_title='Time',
            yaxis_title='Magnitude',
            height=400
        )
        
        return fig
    
    @staticmethod
    def create_daily_trend(df: pd.DataFrame, metric: str = 'avg_magnitude') -> go.Figure:
        """Create daily trend chart"""
        if df.empty:
            return go.Figure()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df[metric],
            mode='lines+markers',
            name=metric.replace('_', ' ').title(),
            line=dict(color='#FF6B6B', width=2),
            marker=dict(size=6)
        ))
        
        fig.update_layout(
            title=f'Daily {metric.replace("_", " ").title()}',
            xaxis_title='Date',
            yaxis_title=metric.replace('_', ' ').title(),
            height=350,
            hovermode='x unified'
        )
        
        return fig
    
    @staticmethod
    def create_magnitude_distribution(df: pd.DataFrame) -> go.Figure:
        """Create magnitude distribution chart"""
        if df.empty:
            return go.Figure()
        
        fig = px.bar(
            df,
            x='magnitude_range',
            y='count',
            title='Magnitude Distribution',
            labels={'magnitude_range': 'Magnitude', 'count': 'Count'},
            color='count',
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(height=350)
        
        return fig
    
    @staticmethod
    def create_top_regions_chart(df: pd.DataFrame) -> go.Figure:
        """Create top regions bar chart"""
        if df.empty:
            return go.Figure()
        
        # Shorten long place names
        df['place_short'] = df['place'].str[:50]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=df['place_short'],
            x=df['count'],
            orientation='h',
            marker=dict(
                color=df['avg_magnitude'],
                colorscale='Reds',
                showscale=True,
                colorbar=dict(title="Avg Mag")
            ),
            hovertemplate='<b>%{y}</b><br>Count: %{x}<br>Avg Mag: %{marker.color:.1f}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Top 10 Most Active Regions',
            xaxis_title='Number of Earthquakes',
            yaxis_title='',
            height=400,
            yaxis={'categoryorder': 'total ascending'}
        )
        
        return fig


def main():
    """Main dashboard application"""
    
    # Title and description
    st.title("🌍 Global Earthquake Monitoring Dashboard")
    st.markdown("Real-time earthquake data analysis and visualization")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Settings")
    
    # Database configuration (in production, use secrets)
    if 'db_config' not in st.session_state:
        st.session_state.db_config = {
            'host': st.secrets.get('DB_HOST', 'localhost'),
            'port': st.secrets.get('DB_PORT', 5432),
            'database': st.secrets.get('DB_NAME', 'postgres'),
            'user': st.secrets.get('DB_USER', 'postgres'),
            'password': st.secrets.get('DB_PASSWORD', '')
        }
    
    # Filters
    days_filter = st.sidebar.slider("Days to show", 1, 30, 7)
    min_magnitude = st.sidebar.slider("Minimum magnitude", 0.0, 7.0, 0.0, 0.1)
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        st.sidebar.info("Dashboard will refresh every 30 seconds")
    
    # Initialize data fetcher
    try:
        fetcher = DataFetcher(st.session_state.db_config)
        
        # Fetch data
        with st.spinner("Loading data..."):
            earthquakes_df = fetcher.get_recent_earthquakes(days_filter, min_magnitude)
            daily_stats_df = fetcher.get_daily_stats(days_filter)
            daily_counts_df = fetcher.get_daily_counts(days_filter)
            magnitude_dist_df = fetcher.get_magnitude_distribution(days_filter)
            top_regions_df = fetcher.get_top_regions(days_filter)
        
        # Key metrics
        if not earthquakes_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_earthquakes = len(earthquakes_df)
                st.metric("Total Earthquakes", f"{total_earthquakes:,}")
            
            with col2:
                avg_magnitude = earthquakes_df['mag'].mean()
                st.metric("Avg Magnitude", f"{avg_magnitude:.2f}")
            
            with col3:
                max_magnitude = earthquakes_df['mag'].max()
                st.metric("Max Magnitude", f"{max_magnitude:.1f}")
            
            with col4:
                recent_24h = len(earthquakes_df[
                    pd.to_datetime(earthquakes_df['time']) >= 
                    datetime.now() - timedelta(days=1)
                ])
                st.metric("Last 24 Hours", f"{recent_24h:,}")
        
        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["🗺️ Map View", "📊 Analytics", "📋 Data Table"])
        
        with tab1:
            st.subheader("Earthquake Locations")
            if not earthquakes_df.empty:
                viz = DashboardVisualizer()
                map_fig = viz.create_map(earthquakes_df)
                st.plotly_chart(map_fig, use_container_width=True)
                
                # Timeline
                timeline_fig = viz.create_timeline(earthquakes_df)
                st.plotly_chart(timeline_fig, use_container_width=True)
            else:
                st.info("No earthquake data available for the selected filters")
        
        with tab2:
            st.subheader("Statistical Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Daily trends
                if not daily_stats_df.empty:
                    viz = DashboardVisualizer()
                    trend_fig = viz.create_daily_trend(daily_stats_df, 'avg_magnitude')
                    st.plotly_chart(trend_fig, use_container_width=True)
                
                # Magnitude distribution
                if not magnitude_dist_df.empty:
                    dist_fig = viz.create_magnitude_distribution(magnitude_dist_df)
                    st.plotly_chart(dist_fig, use_container_width=True)
            
            with col2:
                # Daily counts
                if not daily_counts_df.empty:
                    count_fig = viz.create_daily_trend(daily_counts_df, 'count')
                    st.plotly_chart(count_fig, use_container_width=True)
                
                # Top regions
                if not top_regions_df.empty:
                    regions_fig = viz.create_top_regions_chart(top_regions_df)
                    st.plotly_chart(regions_fig, use_container_width=True)
        
        with tab3:
            st.subheader("Recent Earthquakes Data")
            if not earthquakes_df.empty:
                # Format the dataframe
                display_df = earthquakes_df.copy()
                display_df['time'] = pd.to_datetime(display_df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
                display_df = display_df[['time', 'mag', 'place', 'depth', 'latitude', 'longitude', 'mag_type']]
                display_df.columns = ['Time', 'Magnitude', 'Location', 'Depth (km)', 'Latitude', 'Longitude', 'Type']
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=500
                )
                
                # Download button
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"earthquakes_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No data to display")
        
        # Close connection
        fetcher.close()
        
        # Auto-refresh logic
        if auto_refresh:
            import time
            time.sleep(30)
            st.rerun()
    
    except Exception as e:
        st.error(f"❌ Error connecting to database: {str(e)}")
        st.info("Please check your database configuration in the sidebar or Streamlit secrets.")


if __name__ == "__main__":
    main()