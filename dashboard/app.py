"""AirWatch: Urban Pollution Hotspot Detection and Spatiotemporal Analytics.

Main Streamlit Application (Member 3).
Connects PySpark analytics Parquet outputs with an environmental monitoring dashboard.
"""
from __future__ import annotations

import datetime
from pathlib import Path
import sys
import pandas as pd
import streamlit as st

# Ensure project root is in sys.path so 'dashboard' module is found regardless of working directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Configure Streamlit page settings
st.set_page_config(
    page_title="AirWatch — Environmental Analytics",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Resilient import for streamlit-folium
try:
    from streamlit_folium import st_folium
    HAS_ST_FOLIUM = True
except ImportError:
    HAS_ST_FOLIUM = False

from dashboard.charts import (
    create_daily_trend_chart,
    create_day_hour_heatmap,
    create_episode_duration_hist,
    create_episode_frequency_bar,
    create_episode_severity_scatter,
    create_episode_timeline,
    create_hotspot_classification_donut,
    create_hourly_trend_chart,
    create_monthly_trend_chart,
    create_station_comparison_chart,
    create_threshold_sensitivity_chart,
    create_top_hotspots_bar,
    create_weekday_weekend_chart,
)
from dashboard.data_loader import (
    DEFAULT_ANALYTICS_DIR,
    check_analytics_status,
    compute_executive_kpis,
    get_available_cities,
    get_available_pollutants,
    get_available_states,
    load_all_analytics,
)
from dashboard.map_visualization import create_hotspot_map
from dashboard.utils import (
    CLASSIFICATION_COLORS,
    CLASSIFICATION_DESCRIPTIONS,
    CLASSIFICATION_LABELS,
    DEFAULT_THRESHOLDS,
    POLLUTANTS,
    filter_by_city,
    filter_by_state,
    format_concentration,
    format_hours,
    format_number,
    format_percentage,
    get_api_key,
    get_classification_badge,
    get_threshold,
    get_unit,
)

# Custom CSS for Professional Environmental Monitoring UI
CUSTOM_CSS = """
<style>
    /* Clean layout and typography */
    .block-container {
        padding-top: 1.8rem;
        padding-bottom: 2.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
    }

    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #0F172A;
        color: #F8FAFC;
    }
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #F8FAFC !important;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #CBD5E1 !important;
    }

    /* Card Component */
    .metric-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
        margin-bottom: 12px;
    }
    .metric-title {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748B;
        font-weight: 600;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.7rem;
        font-weight: 700;
        color: #0F172A;
        line-height: 1.2;
    }
    .metric-subtext {
        font-size: 0.78rem;
        color: #94A3B8;
        margin-top: 4px;
    }

    /* Section Banner */
    .section-header {
        border-left: 4px solid #0D9488;
        padding-left: 12px;
        margin-bottom: 18px;
        margin-top: 8px;
    }
    .section-header h2 {
        font-size: 1.4rem;
        font-weight: 700;
        color: #0F172A;
        margin: 0;
    }
    .section-header p {
        font-size: 0.9rem;
        color: #64748B;
        margin: 2px 0 0 0;
    }

    /* Callout Card */
    .callout-box {
        background-color: #F0FDF4;
        border: 1px solid #BBF7D0;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 16px;
        color: #166534;
        font-size: 0.88rem;
    }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def render_metric_card(title: str, value: str, subtext: str = "", delta: str | None = None) -> None:
    """Render a styled metric card."""
    delta_html = f'<div style="font-size: 0.75rem; color: #0D9488; font-weight: 600;">{delta}</div>' if delta else ""
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-subtext">{subtext}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chart(fig, **kwargs) -> None:
    """Render Plotly figure with forward and backward compatibility across Streamlit versions."""
    try:
        st.plotly_chart(fig, width="stretch", **kwargs)
    except TypeError:
        st.plotly_chart(fig, use_container_width=True, **kwargs)


def render_dataframe(df, **kwargs) -> None:
    """Render DataFrame with forward and backward compatibility across Streamlit versions."""
    try:
        st.dataframe(df, width="stretch", **kwargs)
    except TypeError:
        st.dataframe(df, use_container_width=True, **kwargs)


# -------------------------------------------------------------------------------------------------
# DATA LOADING & INITIALIZATION
# -------------------------------------------------------------------------------------------------

data = load_all_analytics()
status = check_analytics_status()
all_analytics_ready = any(status.values())

# -------------------------------------------------------------------------------------------------
# SIDEBAR: NAVIGATION & GLOBAL FILTERS
# -------------------------------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="padding-bottom: 12px; border-bottom: 1px solid #334155; margin-bottom: 16px;">
            <div style="font-size: 1.35rem; font-weight: 800; color: #38BDF8; letter-spacing: -0.02em;">
                AirWatch
            </div>
            <div style="font-size: 0.75rem; color: #94A3B8; margin-top: 2px;">
                Urban Pollution Hotspot Analytics
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    page = st.radio(
        "Navigation",
        [
            "Overview",
            "Spatiotemporal Analysis",
            "Pollution Hotspots",
            "Pollution Episodes",
            "Station Comparison",
            "About & Methodology",
        ],
        index=0,
    )

    st.markdown("<hr style='border-color: #334155; margin: 16px 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='font-size: 0.82rem; font-weight: 600; color: #94A3B8; margin-bottom: 8px;'>GLOBAL FILTERS</div>", unsafe_allow_html=True)

    available_pollutants = get_available_pollutants(data)
    selected_pollutant = st.selectbox(
        "Select Pollutant",
        options=available_pollutants,
        index=0 if "PM2.5" in available_pollutants else 0,
        help="Harmonized core pollutant parameters from CPCB CAAQM monitoring network.",
    )

    available_states = get_available_states(data)
    selected_state = st.selectbox(
        "Filter by State",
        options=available_states,
        index=0,
    )

    available_cities = get_available_cities(data, selected_state)
    selected_city = st.selectbox(
        "Filter by City",
        options=available_cities,
        index=0,
    )

    st.markdown("<hr style='border-color: #334155; margin: 16px 0;'>", unsafe_allow_html=True)

    # Pipeline status indicator
    if all_analytics_ready:
        st.markdown(
            """
            <div style="background-color: #064E3B; border: 1px solid #059669; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #A7F3D0;">
                <strong>PySpark Engine:</strong> Connected<br>
                Consuming Member 2 Parquet outputs
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div style="background-color: #7F1D1D; border: 1px solid #DC2626; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #FECACA;">
                <strong>Parquet Data Missing:</strong><br>
                Run: <code>python -m analytics.analytics_runner</code>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='border-color: #334155; margin: 16px 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='font-size: 0.82rem; font-weight: 600; color: #94A3B8; margin-bottom: 8px;'>API & ENVIRONMENT STATUS</div>", unsafe_allow_html=True)

    carto_key = get_api_key("CARTO_API_KEY")
    aqi_key = get_api_key("AQI_API_KEY")

    if carto_key:
        masked_carto = carto_key[:6] + "..." + carto_key[-4:] if len(carto_key) > 10 else "Active"
        st.markdown(
            f"""
            <div style="background-color: #0C4A6E; border: 1px solid #0284C7; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #E0F2FE; margin-bottom: 8px;">
                <strong>CARTO Basemap:</strong> Key Active<br>
                <span style="font-size: 0.7rem; color: #BAE6FD;">Key: <code>{masked_carto}</code> (Positron HD)</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div style="background-color: #1E293B; border: 1px solid #475569; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #CBD5E1; margin-bottom: 8px;">
                <strong>CARTO Basemap:</strong> OpenStreetMap Default<br>
                <span style="font-size: 0.7rem; color: #94A3B8;">Free standard tiles (no key needed)</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if aqi_key:
        masked_aqi = aqi_key[:6] + "..." + aqi_key[-4:] if len(aqi_key) > 10 else "Active"
        st.markdown(
            f"""
            <div style="background-color: #064E3B; border: 1px solid #059669; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #A7F3D0; margin-bottom: 8px;">
                <strong>xKDR / CPCB API:</strong> Configured<br>
                <span style="font-size: 0.7rem; color: #6EE7B7;">Key: <code>{masked_aqi}</code></span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div style="background-color: #1E293B; border: 1px solid #475569; border-radius: 6px; padding: 8px 12px; font-size: 0.75rem; color: #CBD5E1; margin-bottom: 8px;">
                <strong>xKDR Demo Key:</strong> Active (2024 Data)<br>
                <span style="font-size: 0.7rem; color: #94A3B8;">Using built-in demo key</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander("Basemap & Key Settings", expanded=False):
        basemap_style = st.selectbox(
            "Basemap Style",
            options=["Positron", "Dark Matter", "Voyager", "OpenStreetMap"],
            index=0,
            help="Select preferred tile styling for interactive maps.",
        )
        carto_key_input = st.text_input(
            "Override CARTO API Key",
            value=carto_key,
            type="password",
            help="Loaded from .env. You can also paste or override here live.",
        )
        active_carto_key = carto_key_input.strip() if carto_key_input else carto_key

# Raw analytical outputs from Member 2's PySpark pipeline
raw_summary_df = data.get("station_summary", pd.DataFrame())
raw_temporal_df = data.get("temporal", pd.DataFrame())
raw_hotspots_df = data.get("hotspots", pd.DataFrame())
raw_episodes_df = data.get("episodes", pd.DataFrame())
raw_spatial_df = data.get("spatial_hotspots", pd.DataFrame())
raw_sensitivity_df = data.get("threshold_sensitivity", pd.DataFrame())


# Geographic filter helper to dynamically slice datasets by State and City
def apply_geo_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame by selected state and city if geographic columns exist."""
    if df.empty:
        return df
    filtered = filter_by_state(df, selected_state)
    filtered = filter_by_city(filtered, selected_city)
    return filtered


# Apply geographic filters (State & City) across all spatial datasets
summary_df = apply_geo_filter(raw_summary_df)
hotspots_df = apply_geo_filter(raw_hotspots_df)
episodes_df = apply_geo_filter(raw_episodes_df)
spatial_df = apply_geo_filter(raw_spatial_df)
sensitivity_df = apply_geo_filter(raw_sensitivity_df)

# Temporal analytics represent the nationwide macro-baseline aggregated by PySpark across all 538 stations
temporal_df = raw_temporal_df

kpi = compute_executive_kpis(data, selected_pollutant, selected_state, selected_city)
unit = get_unit(selected_pollutant)
threshold = get_threshold(selected_pollutant)

# -------------------------------------------------------------------------------------------------
# 1. OVERVIEW / EXECUTIVE DASHBOARD
# -------------------------------------------------------------------------------------------------

if page == "Overview":
    st.markdown(
        """
        <div class="section-header">
            <h2>Executive Environmental Overview</h2>
            <p>AirWatch spatiotemporal monitoring, hotspot tracking, and episode detection across India's CPCB CAAQM network.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Active filter scope indicator
    scope_desc = "All India (538 Stations)"
    if selected_state != "All States":
        scope_desc = f"State: <strong>{selected_state}</strong>"
        if selected_city != "All Cities":
            scope_desc += f" &bull; City: <strong>{selected_city}</strong>"
    st.markdown(
        f"<div style='font-size: 0.82rem; color: #64748B; margin-top: -10px; margin-bottom: 16px;'>"
        f"📍 <strong>Active Analysis Scope:</strong> {scope_desc} &nbsp;|&nbsp; <strong>Pollutant:</strong> <strong>{selected_pollutant}</strong>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # KPI Summary Cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        render_metric_card(
            f"Mean {selected_pollutant}",
            format_concentration(kpi["mean_concentration"], selected_pollutant),
            f"Threshold: {threshold} {unit}",
        )
    with c2:
        render_metric_card(
            f"Max {selected_pollutant} Peak",
            format_concentration(kpi["max_concentration"], selected_pollutant),
            "Highest observed station reading",
        )
    with c3:
        render_metric_card(
            "Monitored Stations",
            format_number(kpi["stations_count"]),
            f"State: {selected_state}",
        )
    with c4:
        render_metric_card(
            "Identified Hotspots",
            format_number(kpi["hotspot_count"]),
            "Persistent + Frequently elevated",
        )
    with c5:
        render_metric_card(
            "Detected Episodes",
            format_number(kpi["episode_count"]),
            "Consecutive elevated periods",
        )

    st.markdown("---")

    # Main Visualizations Row
    col_left, col_right = st.columns([3, 2])

    with col_left:
        render_chart(create_daily_trend_chart(temporal_df, selected_pollutant))

    with col_right:
        render_chart(create_top_hotspots_bar(hotspots_df, selected_pollutant, top_n=8))

    # Secondary Row: Hotspot Map Preview & Analytical Insights
    col_map, col_info = st.columns([3, 2])

    with col_map:
        st.markdown("##### Geographic Hotspot Distribution Preview")
        if active_carto_key:
            st.caption("Basemap: **CARTO Positron HD** (API key verified)")
        else:
            st.caption("Basemap: **OpenStreetMap Standard**")
        m = create_hotspot_map(
            spatial_df,
            selected_pollutant,
            state=selected_state,
            city=selected_city,
            carto_api_key=active_carto_key,
            basemap_style=basemap_style,
        )
        if HAS_ST_FOLIUM:
            st_folium(m, height=380, width="100%", returned_objects=[])
        else:
            st.components.v1.html(m._repr_html_(), height=380)

    with col_info:
        st.markdown("##### Big Data Analytical Findings")
        st.markdown(
            f"""
            <div class="callout-box">
                <strong>Data Processing Summary:</strong><br>
                Cleaned dataset contains <strong>26,904,967</strong> valid hourly observations across <strong>538</strong> CPCB continuous ambient air quality monitors in 2024.
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            - **Pollutant Behavior:** {selected_pollutant} concentration threshold is set to **{threshold} {unit}** as defined in Member 2's PySpark analytics.
            - **Persistence Dynamics:** Stations are classified as persistent hotspots only when elevated readings exceed 50% persistence with high observed magnitude (>= 1.5x threshold).
            - **Temporal Seasonality:** Extreme pollution episodes cluster predominantly during post-monsoon and winter months (October through January) due to meteorology and boundary-layer inversion.
            - **Observation Reliability:** All station rankings and classifications enforce a minimum observation constraint (>= 24 hours) to prevent misleading conclusions from sparse series.
            """
        )

# -------------------------------------------------------------------------------------------------
# 2. SPATIOTEMPORAL ANALYSIS
# -------------------------------------------------------------------------------------------------

elif page == "Spatiotemporal Analysis":
    st.markdown(
        """
        <div class="section-header">
            <h2>Spatiotemporal Analytics</h2>
            <p>Diurnal profiles, long-term trends, seasonal dynamics, and weekly emission variability.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.info(
        f"💡 **Macro Temporal Baseline:** Diurnal (hourly), daily anomaly, seasonal (monthly), and weekly curves represent the **nationwide baseline across all 538 stations in India** pre-aggregated by Apache PySpark ({selected_pollutant}). For state and city-level breakdowns, explore the **Pollution Hotspots**, **Pollution Episodes**, and **Station Comparison** tabs which filter dynamically by State and City."
    )

    tabs = st.tabs([
        "Diurnal (Hourly) Cycle",
        "Daily Timeline & Anomalies",
        "Seasonal & Monthly Trends",
        "Weekday vs Weekend & Heatmap",
    ])

    with tabs[0]:
        render_chart(create_hourly_trend_chart(temporal_df, selected_pollutant))
        st.markdown(
            f"""
            **Diurnal Insights:**
            - Urban concentrations of {selected_pollutant} typically peak during early morning (07:00–10:00 IST) and late evening (20:00–23:00 IST).
            - This bimodal distribution reflects combined impacts of vehicular rush-hour emissions and shallow nocturnal atmospheric boundary layer heights that trap pollutants near the surface.
            """
        )

    with tabs[1]:
        # Date filter controls
        c_d1, c_d2 = st.columns(2)
        with c_d1:
            start_date = st.date_input("Start Date", value=datetime.date(2024, 1, 1))
        with c_d2:
            end_date = st.date_input("End Date", value=datetime.date(2024, 12, 31))

        render_chart(
            create_daily_trend_chart(temporal_df, selected_pollutant, start_date=start_date, end_date=end_date)
        )
        st.markdown(
            """
            **Statistical Anomaly Flagging:**
            - Red circular markers denote periods where the daily concentration Z-score is ≥ 2.0 (p < 0.05).
            - These indicate statistically significant surges relative to normal seasonal expectations.
            """
        )

    with tabs[2]:
        col_m1, col_m2 = st.columns([3, 2])
        with col_m1:
            render_chart(create_monthly_trend_chart(temporal_df, selected_pollutant))
        with col_m2:
            st.markdown("##### Monthly Concentration Statistics")
            if not temporal_df.empty:
                m_table = temporal_df[
                    (temporal_df["temporal_granularity"] == "monthly")
                    & (temporal_df["parameter"] == selected_pollutant)
                ][["period", "mean_concentration", "max_concentration", "observation_count"]].rename(
                    columns={
                        "period": "Month",
                        "mean_concentration": f"Mean ({unit})",
                        "max_concentration": f"Peak ({unit})",
                        "observation_count": "Observations",
                    }
                )
                render_dataframe(m_table, hide_index=True)

    with tabs[3]:
        col_w1, col_w2 = st.columns([2, 3])
        with col_w1:
            render_chart(create_weekday_weekend_chart(temporal_df, selected_pollutant))
        with col_w2:
            render_chart(create_day_hour_heatmap(temporal_df, selected_pollutant))

# -------------------------------------------------------------------------------------------------
# 3. POLLUTION HOTSPOT ANALYSIS
# -------------------------------------------------------------------------------------------------

elif page == "Pollution Hotspots":
    st.markdown(
        """
        <div class="section-header">
            <h2>Geographic & Persistence Hotspot Detection</h2>
            <p>Spatial station mapping, persistence scoring, and classification distribution.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    scope_desc = "All India (538 Stations)"
    if selected_state != "All States":
        scope_desc = f"State: <strong>{selected_state}</strong>"
        if selected_city != "All Cities":
            scope_desc += f" &bull; City: <strong>{selected_city}</strong>"
    st.markdown(
        f"<div style='font-size: 0.82rem; color: #64748B; margin-top: -10px; margin-bottom: 14px;'>"
        f"📍 <strong>Analysis Scope:</strong> {scope_desc} &nbsp;|&nbsp; <strong>Pollutant:</strong> <strong>{selected_pollutant}</strong>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Classification filter
    selected_classes = st.multiselect(
        "Filter by Hotspot Classification",
        options=list(CLASSIFICATION_LABELS.keys()),
        default=list(CLASSIFICATION_LABELS.keys()),
        format_func=lambda x: CLASSIFICATION_LABELS.get(x, x),
    )

    # Interactive Map
    st.markdown("##### Interactive Station Hotspot Map")
    if active_carto_key:
        st.caption("Basemap: **CARTO Positron HD** (API key verified from `.env`)")
    else:
        st.caption("Basemap: **OpenStreetMap Standard**")
    m = create_hotspot_map(
        spatial_df,
        selected_pollutant,
        selected_classifications=selected_classes,
        state=selected_state,
        city=selected_city,
        carto_api_key=active_carto_key,
        basemap_style=basemap_style,
    )
    if HAS_ST_FOLIUM:
        st_folium(m, height=520, width="100%", returned_objects=[])
    else:
        st.components.v1.html(m._repr_html_(), height=520)

    st.markdown("---")

    # Classification Distribution and Top Hotspots
    col_pie, col_bar = st.columns(2)
    with col_pie:
        render_chart(create_hotspot_classification_donut(hotspots_df, selected_pollutant))
    with col_bar:
        render_chart(create_top_hotspots_bar(hotspots_df, selected_pollutant, top_n=10))

    # Classification Methodology Explanatory Card
    with st.expander("Analytical Classification Methodology Details", expanded=False):
        st.markdown(
            f"""
            Member 2's PySpark analytics classifies monitoring stations based on **persistence, magnitude, and coverage sufficiency**:
            - **Baseline Threshold:** {selected_pollutant} threshold is {threshold} {unit}.
            - **Persistence Score:** Defined as `100 * (elevated_observations / valid_observations)`.
            - **Persistent Hotspot:** Station has ≥ 24 valid observations, persistence score ≥ 50%, mean concentration ≥ threshold, and at least one peak ≥ 1.5× threshold.
            - **Frequently Elevated:** Persistence score ≥ 25%, indicating recurring pollution exposure.
            - **Isolated Spike:** Maximum concentration exceeded threshold, but overall persistence remained below 25%.
            - **Ordinary Fluctuation:** All observed readings remained below the analytical threshold.
            - **Insufficient Observations:** Stations with fewer than 24 readings are excluded from hotspot classification to prevent false positives.
            """
        )

    # Station Details Table
    st.markdown("##### Station Hotspot Records")
    if not hotspots_df.empty:
        h_filtered = hotspots_df[hotspots_df["parameter"] == selected_pollutant].copy()
        if selected_state != "All States" and "state" in h_filtered.columns:
            h_filtered = h_filtered[h_filtered["state"] == selected_state]
        if selected_city != "All Cities" and "city" in h_filtered.columns:
            h_filtered = h_filtered[h_filtered["city"] == selected_city]
        if selected_classes:
            h_filtered = h_filtered[h_filtered["classification"].isin(selected_classes)]

        display_cols = [
            "station_id", "station_name", "city", "state",
            "mean_concentration", "max_concentration", "persistence_score",
            "valid_observations", "classification"
        ]
        available_cols = [c for c in display_cols if c in h_filtered.columns]
        render_dataframe(
            h_filtered[available_cols].rename(
                columns={
                    "mean_concentration": f"Mean ({unit})",
                    "max_concentration": f"Max ({unit})",
                    "persistence_score": "Persistence (%)",
                    "valid_observations": "Observations",
                }
            ),
            hide_index=True,
        )

# -------------------------------------------------------------------------------------------------
# 4. POLLUTION EPISODE ANALYSIS
# -------------------------------------------------------------------------------------------------

elif page == "Pollution Episodes":
    st.markdown(
        """
        <div class="section-header">
            <h2>Pollution Episode Mining & Sensitivity Analysis</h2>
            <p>Consecutive elevated measurement runs, episode durations, and threshold sensitivity.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    scope_desc = "All India"
    if selected_state != "All States":
        scope_desc = f"State: <strong>{selected_state}</strong>"
        if selected_city != "All Cities":
            scope_desc += f" &bull; City: <strong>{selected_city}</strong>"
    st.markdown(
        f"<div style='font-size: 0.82rem; color: #64748B; margin-top: -10px; margin-bottom: 14px;'>"
        f"📍 <strong>Analysis Scope:</strong> {scope_desc} &nbsp;|&nbsp; <strong>Pollutant:</strong> <strong>{selected_pollutant}</strong>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Episode KPIs
    filtered_episodes = episodes_df[episodes_df["parameter"] == selected_pollutant] if not episodes_df.empty else pd.DataFrame()
    total_episodes = len(filtered_episodes)
    avg_duration = filtered_episodes["duration_hours"].mean() if not filtered_episodes.empty else 0.0
    max_duration = filtered_episodes["duration_hours"].max() if not filtered_episodes.empty else 0.0
    worst_peak = filtered_episodes["maximum_concentration"].max() if not filtered_episodes.empty else 0.0

    ec1, ec2, ec3, ec4 = st.columns(4)
    with ec1:
        render_metric_card("Total Detected Episodes", format_number(total_episodes), f"Parameter: {selected_pollutant}")
    with ec2:
        render_metric_card("Average Episode Duration", format_hours(avg_duration), "Consecutive hours")
    with ec3:
        render_metric_card("Longest Episode Run", format_hours(max_duration), "Maximum duration observed")
    with ec4:
        render_metric_card("Peak Episode Concentration", format_concentration(worst_peak, selected_pollutant), "Highest during an episode")

    st.markdown("---")

    col_ep1, col_ep2 = st.columns(2)
    with col_ep1:
        render_chart(create_episode_duration_hist(episodes_df, selected_pollutant))
    with col_ep2:
        render_chart(create_episode_frequency_bar(episodes_df, selected_pollutant, top_n=10))

    # Episode Severity and Timeline
    render_chart(create_episode_severity_scatter(episodes_df, selected_pollutant))
    render_chart(create_episode_timeline(episodes_df, selected_pollutant))

    st.markdown("---")

    # Threshold Sensitivity Analysis Section
    st.markdown("##### Threshold Sensitivity Analysis")
    st.markdown(
        """
        To verify whether hotspot classifications depend artificially on arbitrary threshold cutoffs,
        Member 2 evaluated the dataset against three sensitivity regimes: **Lenient (0.8x)**, **Baseline (1.0x)**, and **Strict (1.2x)**.
        """
    )
    render_chart(create_threshold_sensitivity_chart(sensitivity_df, selected_pollutant))

    # Episode Records Table
    with st.expander("Browse Detected Pollution Episodes Table", expanded=False):
        if not filtered_episodes.empty:
            render_dataframe(
                filtered_episodes.sort_values("duration_hours", ascending=False).head(100),
                hide_index=True,
            )

# -------------------------------------------------------------------------------------------------
# 5. STATION COMPARISON
# -------------------------------------------------------------------------------------------------

elif page == "Station Comparison":
    st.markdown(
        """
        <div class="section-header">
            <h2>Station Comparative Analytics</h2>
            <p>Direct head-to-head station comparisons with observation count reliability signals.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    scope_desc = "All India"
    if selected_state != "All States":
        scope_desc = f"State: <strong>{selected_state}</strong>"
        if selected_city != "All Cities":
            scope_desc += f" &bull; City: <strong>{selected_city}</strong>"
    st.markdown(
        f"<div style='font-size: 0.82rem; color: #64748B; margin-top: -10px; margin-bottom: 14px;'>"
        f"📍 <strong>Analysis Scope:</strong> {scope_desc} &nbsp;|&nbsp; <strong>Pollutant:</strong> <strong>{selected_pollutant}</strong>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Multi-station selector
    if not summary_df.empty:
        param_summary = summary_df[summary_df["parameter"] == selected_pollutant].copy()
        all_stations = sorted(param_summary["station_name"].dropna().unique().tolist())

        if not all_stations:
            loc_label = selected_city if selected_city != "All Cities" else selected_state
            st.info(f"No monitoring stations found for {selected_pollutant} in {loc_label}.")
        else:
            default_selection = all_stations[:min(4, len(all_stations))]

            selected_stations = st.multiselect(
                "Select Monitoring Stations to Compare",
                options=all_stations,
                default=default_selection,
            )

            if selected_stations:
                render_chart(
                    create_station_comparison_chart(summary_df, selected_stations, selected_pollutant)
                )

                # Comparative Metrics Table with reliability signals
                st.markdown("##### Station Reliability & Comparative Metrics")
                comp_table = param_summary[param_summary["station_name"].isin(selected_stations)][
                    [
                        "station_name", "city", "state", "mean_concentration",
                        "max_concentration", "stddev_concentration",
                        "observation_count", "active_days"
                    ]
                ].rename(
                    columns={
                        "station_name": "Station",
                        "city": "City",
                        "state": "State",
                        "mean_concentration": f"Mean ({unit})",
                        "max_concentration": f"Max ({unit})",
                        "stddev_concentration": f"Std Dev ({unit})",
                        "observation_count": "Readings",
                        "active_days": "Active Days",
                    }
                )
                render_dataframe(comp_table, hide_index=True)

                st.markdown(
                    """
                    > [!NOTE]
                    > **Observation Reliability:** Stations with fewer active days or lower reading counts can display high mean concentrations due to localized sampling bias. Always evaluate reading counts alongside concentration rankings.
                    """
                )
            else:
                st.warning("Please select at least one monitoring station from the dropdown to compare.")
    else:
        st.warning("Station summary data not available.")

# -------------------------------------------------------------------------------------------------
# 6. ABOUT & METHODOLOGY (ACADEMIC / VIVA REQUIREMENTS)
# -------------------------------------------------------------------------------------------------

elif page == "About & Methodology":
    st.markdown(
        """
        <div class="section-header">
            <h2>AirWatch: Project Information & Academic Documentation</h2>
            <p>BDSMA Microproject: Spatiotemporal Big Data Analytics & Urban Pollution Hotspot Detection</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_a1, col_a2 = st.columns(2)

    with col_a1:
        st.markdown(
            """
            ### Project Overview
            - **Title:** AirWatch: Urban Pollution Hotspot Detection Using Big Data Analytics
            - **Category:** Spatiotemporal Analytics
            - **Academic Scope:** 4th Year Computer Engineering Microproject (BDSMA)
            - **Dataset Source:** India Air Quality Database (xKDR Forum / CPCB CAAQM Network)
            - **Scale:** 46.9 Million raw observations cleaned down to 26.9 Million analysis-ready records across 538+ monitoring stations for the calendar year 2024.

            ### Technology Stack
            - **Big Data Processing:** Apache PySpark 4.0 (Distributed Transformations, Window Aggregations)
            - **Storage Format:** Apache Parquet (Columnar, Snappy-compressed partitions)
            - **Dashboard Framework:** Streamlit (Multi-page reactive UI)
            - **Interactive Visualizations:** Plotly Graph Objects & Plotly Express
            - **Spatiotemporal Mapping:** Folium & Leaflet.js
            - **Containerization:** Docker & Docker Compose
            """
        )

    with col_a2:
        st.markdown(
            """
            ### Sustainable Development Goals (SDGs)
            - **SDG 11: Sustainable Cities and Communities (Target 11.6):**
              By 2030, reduce the adverse per capita environmental impact of cities, including by paying special attention to air quality and municipal waste management. AirWatch provides actionable hotspot detection enabling city administrations to implement targeted emission-abatement strategies.
            - **SDG 3: Good Health and Well-being (Target 3.9):**
              By 2030, substantially reduce the number of deaths and illnesses from hazardous chemicals and air, water, and soil pollution and contamination. By identifying prolonged, consecutive pollution episodes, AirWatch highlights acute exposure zones affecting urban public health.
            """
        )

    st.markdown("---")

    st.markdown(
        """
        ### Big Data Processing Architecture
        1. **Raw Ingestion:** Partitioned Parquet data read via PySpark from xKDR CAAQM archive (`year=2024/month=*`).
        2. **Cleaning & Sanity Bounds:** Conservative pollutant ceiling filters, stuck-sensor removal (24+ consecutive invariant readings), coordinate bounding validation (Lat 6–38°N, Lon 68–98°E).
        3. **Distributed Analytics:** PySpark window functions compute diurnal cycles, rolling statistics, persistence ratios, and consecutive episode clusters without memory exhaustion.
        4. **Analytical Parquet Outputs:** Results written to `data/processed/analytics/` as compact, pre-aggregated Parquet tables.
        5. **Dashboard Layer:** Streamlit and Plotly consume these aggregated tables efficiently via pandas/pyarrow, eliminating redundant Big Data recomputation.
        """
    )
