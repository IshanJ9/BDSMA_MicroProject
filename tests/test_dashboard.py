"""Automated tests for the AirWatch dashboard layer (Member 3).

Tests data loaders, utility formatters, Plotly chart builders,
and Folium map generation.
"""
from __future__ import annotations

import datetime
from pathlib import Path
import sys
import folium
import pandas as pd
import plotly.graph_objects as go
import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
    _read_parquet_safe,
    check_analytics_status,
    compute_executive_kpis,
    get_available_cities,
    get_available_pollutants,
    get_available_states,
)
from dashboard.map_visualization import create_hotspot_map
from dashboard.utils import (
    DEFAULT_THRESHOLDS,
    POLLUTANTS,
    filter_by_city,
    filter_by_classifications,
    filter_by_date_range,
    filter_by_pollutant,
    filter_by_state,
    format_concentration,
    format_hours,
    format_number,
    format_percentage,
    get_api_key,
    get_classification_badge,
    get_classification_color,
    get_classification_label,
    get_threshold,
    get_unit,
    load_project_env,
)


def test_env_and_api_keys():
    env_vars = load_project_env()
    assert isinstance(env_vars, dict)
    # Check that get_api_key returns a string
    carto = get_api_key("CARTO_API_KEY")
    assert isinstance(carto, str)


# -------------------------------------------------------------------------------------------------
# 1. UTILITY & FORMATTING TESTS
# -------------------------------------------------------------------------------------------------

def test_pollutant_units_and_thresholds():
    assert get_unit("PM2.5") == "µg/m³"
    assert get_unit("CO") == "mg/m³"
    assert get_unit("Unknown") == "µg/m³"

    assert get_threshold("PM2.5") == 60.0
    assert get_threshold("CO") == 4.0
    assert get_threshold("Unknown") == 60.0


def test_formatting_functions():
    assert format_concentration(52.34, "PM2.5") == "52.3 µg/m³"
    assert format_concentration(None, "PM2.5") == "N/A"

    assert format_number(1234567) == "1,234,567"
    assert format_number(1234.56, decimals=1) == "1,234.6"
    assert format_number(None) == "N/A"

    assert format_percentage(45.67) == "45.7%"
    assert format_percentage(None) == "N/A"

    assert format_hours(0.5) == "30 mins"
    assert format_hours(4.5) == "4.5 hrs"
    assert format_hours(None) == "N/A"


def test_classification_metadata_and_badge():
    color = get_classification_color("persistent_hotspot")
    assert color == "#D32F2F"

    label = get_classification_label("persistent_hotspot")
    assert label == "Persistent Hotspot"

    badge = get_classification_badge("persistent_hotspot")
    assert "Persistent Hotspot" in badge
    assert "#D32F2F" in badge


def test_dataframe_filters():
    df = pd.DataFrame(
        {
            "parameter": ["PM2.5", "PM2.5", "NO2"],
            "state": ["Maharashtra", "Delhi", "Delhi"],
            "city": ["Mumbai", "Delhi", "Delhi"],
            "classification": ["persistent_hotspot", "frequently_elevated", "isolated_spike"],
            "period": ["2024-01-01", "2024-03-01", "2024-06-01"],
        }
    )

    filtered_p = filter_by_pollutant(df, "PM2.5")
    assert len(filtered_p) == 2

    filtered_s = filter_by_state(df, "Delhi")
    assert len(filtered_s) == 2

    filtered_c = filter_by_city(df, "Mumbai")
    assert len(filtered_c) == 1

    filtered_cls = filter_by_classifications(df, ["persistent_hotspot"])
    assert len(filtered_cls) == 1

    filtered_d = filter_by_date_range(
        df,
        start_date=datetime.date(2024, 2, 1),
        end_date=datetime.date(2024, 5, 1),
        date_col="period",
    )
    assert len(filtered_d) == 1
    assert filtered_d.iloc[0]["period"] == "2024-03-01"


# -------------------------------------------------------------------------------------------------
# 2. DATA LOADER TESTS
# -------------------------------------------------------------------------------------------------

def test_read_parquet_safe_missing_path():
    df = _read_parquet_safe("non_existent_directory_xyz", fallback_columns=["col_a", "col_b"])
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col_a", "col_b"]
    assert df.empty


def test_check_analytics_status():
    status = check_analytics_status()
    assert isinstance(status, dict)
    expected_keys = [
        "temporal", "station_summary", "station_rankings",
        "hotspots", "episodes", "spatial_hotspots", "threshold_sensitivity"
    ]
    for key in expected_keys:
        assert key in status


def test_compute_executive_kpis():
    summary = pd.DataFrame(
        {
            "station_id": ["site_1", "site_2"],
            "parameter": ["PM2.5", "PM2.5"],
            "state": ["Delhi", "Delhi"],
            "city": ["Delhi", "Delhi"],
            "mean_concentration": [80.0, 100.0],
            "max_concentration": [150.0, 200.0],
            "observation_count": [1000, 1200],
        }
    )
    hotspots = pd.DataFrame(
        {
            "station_id": ["site_1", "site_2"],
            "parameter": ["PM2.5", "PM2.5"],
            "classification": ["persistent_hotspot", "ordinary_fluctuation"],
        }
    )
    episodes = pd.DataFrame(
        {
            "station_id": ["site_1"],
            "parameter": ["PM2.5"],
            "duration_hours": [6.0],
            "average_concentration": [95.0],
        }
    )

    data = {"station_summary": summary, "hotspots": hotspots, "episodes": episodes}
    kpis = compute_executive_kpis(data, "PM2.5")

    assert kpis["stations_count"] == 2
    assert kpis["total_observations"] == 2200
    assert kpis["mean_concentration"] == 90.0
    assert kpis["max_concentration"] == 200.0
    assert kpis["hotspot_count"] == 1
    assert kpis["episode_count"] == 1
    assert kpis["threshold"] == 60.0


# -------------------------------------------------------------------------------------------------
# 3. CHART BUILDER TESTS
# -------------------------------------------------------------------------------------------------

def test_hourly_and_daily_charts():
    temporal_df = pd.DataFrame(
        {
            "temporal_granularity": ["hourly", "hourly", "daily", "daily"],
            "parameter": ["PM2.5", "PM2.5", "PM2.5", "PM2.5"],
            "period": ["0", "1", "2024-01-01", "2024-01-02"],
            "mean_concentration": [50.0, 60.0, 55.0, 75.0],
            "max_concentration": [70.0, 80.0, 90.0, 120.0],
            "min_concentration": [30.0, 40.0, 30.0, 45.0],
            "stddev_concentration": [10.0, 12.0, 15.0, 18.0],
            "observation_count": [100, 100, 500, 500],
            "period_z_score": [0.5, 0.8, 0.2, 2.5],
            "is_unusually_elevated": [False, False, False, True],
        }
    )

    fig_hourly = create_hourly_trend_chart(temporal_df, "PM2.5")
    assert isinstance(fig_hourly, go.Figure)
    assert len(fig_hourly.data) >= 1
    # The axis must follow the mean curve (50-60), not a 0..max band that flattens the daily cycle.
    assert "Min-Max Range" not in [t.name for t in fig_hourly.data]
    assert fig_hourly.layout.yaxis.range[1] < 100

    fig_daily = create_daily_trend_chart(temporal_df, "PM2.5")
    assert isinstance(fig_daily, go.Figure)
    assert len(fig_daily.data) >= 2  # daily mean + 7d rolling + anomaly marker


def test_hotspot_and_episode_charts():
    hotspots_df = pd.DataFrame(
        {
            "station_id": ["s1", "s2", "s3"],
            "station_name": ["Station A", "Station B", "Station C"],
            "parameter": ["PM2.5", "PM2.5", "PM2.5"],
            "persistence_score": [80.0, 40.0, 10.0],
            "mean_concentration": [75.0, 65.0, 30.0],
            "max_concentration": [150.0, 110.0, 50.0],
            "classification": ["persistent_hotspot", "frequently_elevated", "ordinary_fluctuation"],
            "coverage_sufficient": [True, True, True],
        }
    )

    fig_donut = create_hotspot_classification_donut(hotspots_df, "PM2.5")
    assert isinstance(fig_donut, go.Figure)

    fig_top = create_top_hotspots_bar(hotspots_df, "PM2.5", top_n=2)
    assert isinstance(fig_top, go.Figure)

    episodes_df = pd.DataFrame(
        {
            "station_id": ["s1", "s1", "s2"],
            "station_name": ["Station A", "Station A", "Station B"],
            "parameter": ["PM2.5", "PM2.5", "PM2.5"],
            "start_time": pd.to_datetime(["2024-01-05 10:00", "2024-02-10 14:00", "2024-03-01 08:00"]),
            "duration_hours": [4.0, 8.0, 3.0],
            "average_concentration": [85.0, 110.0, 75.0],
            "maximum_concentration": [120.0, 160.0, 95.0],
            "elevated_observations": [4, 8, 3],
        }
    )

    fig_hist = create_episode_duration_hist(episodes_df, "PM2.5")
    assert isinstance(fig_hist, go.Figure)

    fig_freq = create_episode_frequency_bar(episodes_df, "PM2.5")
    assert isinstance(fig_freq, go.Figure)

    fig_scatter = create_episode_severity_scatter(episodes_df, "PM2.5")
    assert isinstance(fig_scatter, go.Figure)


# -------------------------------------------------------------------------------------------------
# 4. MAP VISUALIZATION TESTS
# -------------------------------------------------------------------------------------------------

def test_hotspot_map_builder():
    spatial_df = pd.DataFrame(
        {
            "station_id": ["site_1", "site_2"],
            "station_name": ["Station North", "Station South"],
            "city": ["Delhi", "Mumbai"],
            "state": ["Delhi", "Maharashtra"],
            "parameter": ["PM2.5", "PM2.5"],
            "latitude": [28.6139, 19.0760],
            "longitude": [77.2090, 72.8777],
            "mean_concentration": [85.0, 52.0],
            "max_concentration": [180.0, 95.0],
            "persistence_score": [75.0, 20.0],
            "observation_count": [5000, 4800],
            "classification": ["persistent_hotspot", "ordinary_fluctuation"],
            "coord_source": ["cpcb_registry", "geocoded_city"],
        }
    )

    m = create_hotspot_map(spatial_df, "PM2.5", carto_api_key="test_key")
    assert isinstance(m, folium.Map)
    rendered = m._repr_html_()
    assert "Station North" in rendered or "site_1" in rendered
    assert "Hotspot Classification" in rendered
    assert "cartocdn.com" in rendered or "OpenStreetMap" in rendered
