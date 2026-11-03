"""Utility functions, constants, formatting, and filtering for the AirWatch dashboard.

Part of Member 3's contribution to the AirWatch project.
"""
from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
import pandas as pd


def load_project_env() -> dict[str, str]:
    """Parse .env file from workspace root or container directory directly into os.environ.

    Ensures that newly added or modified keys in .env are immediately recognized
    even inside running Docker containers without needing a container rebuild.
    """
    candidates = [
        Path(__file__).resolve().parents[1] / ".env",
        Path("/app/.env"),
        Path.cwd() / ".env",
    ]
    loaded = {}
    for p in candidates:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            k = k.strip()
                            v = v.strip().strip("'\"")
                            if k and v:
                                os.environ[k] = v
                                loaded[k] = v
                break
            except Exception:
                pass
    return loaded


def get_api_key(name: str, default: str = "") -> str:
    """Retrieve an API key from os.environ or directly from .env file."""
    load_project_env()
    return os.environ.get(name, default).strip()


# Core pollutants analyzed in the AirWatch project
POLLUTANTS: list[str] = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone", "NH3"]

# Standard measurement units harmonized by Member 1's ingestion pipeline
POLLUTANT_UNITS: dict[str, str] = {
    "PM2.5": "µg/m³",
    "PM10": "µg/m³",
    "NO2": "µg/m³",
    "SO2": "µg/m³",
    "CO": "mg/m³",
    "Ozone": "µg/m³",
    "NH3": "µg/m³",
}

# Pollutant-specific concentration thresholds defined in Member 2's analytics
# Note: These are measured concentration thresholds, distinct from multi-pollutant AQI breakpoints.
DEFAULT_THRESHOLDS: dict[str, float] = {
    "PM2.5": 60.0,
    "PM10": 100.0,
    "NO2": 80.0,
    "SO2": 80.0,
    "CO": 4.0,
    "Ozone": 100.0,
    "NH3": 400.0,
}

# Environmental color palette for hotspot classifications
CLASSIFICATION_COLORS: dict[str, str] = {
    "persistent_hotspot": "#D32F2F",       # Crimson / Alert Red
    "frequently_elevated": "#F57C00",      # Deep Orange / Amber
    "isolated_spike": "#FBC02D",           # Gold / Yellow
    "ordinary_fluctuation": "#2E7D32",     # Forest Green / Low Risk
    "insufficient_observations": "#78909C", # Slate Grey / Unclassified
}

# Human-readable labels for analytical classifications
CLASSIFICATION_LABELS: dict[str, str] = {
    "persistent_hotspot": "Persistent Hotspot",
    "frequently_elevated": "Frequently Elevated",
    "isolated_spike": "Isolated Spike",
    "ordinary_fluctuation": "Ordinary Fluctuation",
    "insufficient_observations": "Insufficient Observations",
}

# Explanations for analytical classifications
CLASSIFICATION_DESCRIPTIONS: dict[str, str] = {
    "persistent_hotspot": "Persistence score >= 50%, mean concentration >= threshold, and peak >= 1.5x threshold.",
    "frequently_elevated": "Persistence score >= 25% of valid observations exceed threshold.",
    "isolated_spike": "Maximum concentration exceeded threshold, but persistence was under 25%.",
    "ordinary_fluctuation": "All observed concentrations stayed below the threshold.",
    "insufficient_observations": "Valid readings for under 75% of the period's hours; cannot reliably classify.",
}

# Indian Geographic Boundaries (from pipeline config)
INDIA_BOUNDS = {
    "lat_min": 6.0,
    "lat_max": 38.0,
    "lon_min": 68.0,
    "lon_max": 98.0,
}


def get_unit(pollutant: str) -> str:
    """Return the unit for the given pollutant (default to µg/m³)."""
    return POLLUTANT_UNITS.get(pollutant, "µg/m³")


def get_threshold(pollutant: str) -> float:
    """Return the concentration threshold for the given pollutant."""
    return DEFAULT_THRESHOLDS.get(pollutant, 60.0)


def format_concentration(value: float | None, pollutant: str, decimals: int = 1) -> str:
    """Format a concentration value with its appropriate unit."""
    if value is None or pd.isna(value):
        return "N/A"
    unit = get_unit(pollutant)
    return f"{value:,.{decimals}f} {unit}"


def format_number(value: float | int | None, decimals: int = 1) -> str:
    """Format a numeric value with comma thousands separators."""
    if value is None or pd.isna(value):
        return "N/A"
    if isinstance(value, int) or value.is_integer() if hasattr(value, "is_integer") else False:
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def format_percentage(value: float | None, decimals: int = 1) -> str:
    """Format a percentage value."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{decimals}f}%"


def format_hours(hours: float | None) -> str:
    """Format duration in hours."""
    if hours is None or pd.isna(hours):
        return "N/A"
    if hours < 1.0:
        return f"{int(hours * 60)} mins"
    return f"{hours:.1f} hrs"


def get_classification_color(classification: str) -> str:
    """Return hex color corresponding to hotspot classification."""
    return CLASSIFICATION_COLORS.get(classification, "#78909C")


def get_classification_label(classification: str) -> str:
    """Return human-readable label for hotspot classification."""
    return CLASSIFICATION_LABELS.get(classification, classification.replace("_", " ").title())


def get_classification_badge(classification: str) -> str:
    """Return an HTML badge component for the classification."""
    color = get_classification_color(classification)
    label = get_classification_label(classification)
    return (
        f'<span style="background-color: {color}22; color: {color}; '
        f'border: 1px solid {color}88; padding: 3px 8px; border-radius: 4px; '
        f'font-weight: 600; font-size: 0.82rem;">{label}</span>'
    )


def filter_by_pollutant(df: pd.DataFrame, pollutant: str) -> pd.DataFrame:
    """Filter DataFrame for a specific pollutant if the column exists."""
    if df.empty or "parameter" not in df.columns:
        return df
    return df[df["parameter"] == pollutant].copy()


def filter_by_state(df: pd.DataFrame, state: str | None) -> pd.DataFrame:
    """Filter DataFrame by state."""
    if df.empty or not state or state == "All States" or "state" not in df.columns:
        return df
    return df[df["state"] == state].copy()


def filter_by_city(df: pd.DataFrame, city: str | None) -> pd.DataFrame:
    """Filter DataFrame by city."""
    if df.empty or not city or city == "All Cities" or "city" not in df.columns:
        return df
    return df[df["city"] == city].copy()


def filter_by_classifications(df: pd.DataFrame, classifications: Sequence[str] | None) -> pd.DataFrame:
    """Filter DataFrame by one or more classifications."""
    if df.empty or not classifications or "classification" not in df.columns:
        return df
    return df[df["classification"].isin(classifications)].copy()


def filter_by_date_range(
    df: pd.DataFrame,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    date_col: str = "period",
) -> pd.DataFrame:
    """Filter DataFrame by date range."""
    if df.empty or date_col not in df.columns or (start_date is None and end_date is None):
        return df

    filtered = df.copy()
    # Convert column to datetime for robust comparison
    dates = pd.to_datetime(filtered[date_col], errors="coerce").dt.date
    if start_date is not None:
        filtered = filtered[dates >= start_date]
        dates = dates[dates >= start_date]
    if end_date is not None:
        filtered = filtered[dates <= end_date]

    return filtered
