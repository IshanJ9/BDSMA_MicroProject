"""Folium interactive map module for AirWatch spatiotemporal hotspot visualization (Member 3).

Renders geographic monitoring stations, persistence-based hotspot classifications,
rich diagnostic popups, coordinate provenance flags, and analytical legends.
"""
from __future__ import annotations

import html
import os
from pathlib import Path
import sys
from typing import Sequence
import folium
from folium import plugins
import pandas as pd

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.utils import (
    CLASSIFICATION_COLORS,
    CLASSIFICATION_LABELS,
    format_concentration,
    format_number,
    get_api_key,
    get_classification_color,
    get_classification_label,
    get_threshold,
    get_unit,
)

# Geographic center and default bounds for India
DEFAULT_MAP_CENTER = [22.3511148, 78.6677428]
DEFAULT_ZOOM_START = 5


def _get_base_tiles(custom_key: str | None = None, style: str = "positron") -> dict[str, Any]:
    """Determine the optimal base map layer and attribution.

    If CARTO_API_KEY is configured in the environment / .env / UI override,
    uses CartoDB Positron with the key.
    Otherwise, defaults to OpenStreetMap, which is open-source and requires no API key.
    """
    carto_key = (custom_key or get_api_key("CARTO_API_KEY")).strip()
    if carto_key:
        style_slug = "positron"
        if "dark" in style.lower():
            style_slug = "dark_all"
        elif "voyager" in style.lower():
            style_slug = "voyager"

        return {
            "tiles": f"https://basemaps.cartocdn.com/rastertiles/{style_slug}/{{z}}/{{x}}/{{y}}.png?api_key={carto_key}&key={carto_key}",
            "attr": "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors &copy; <a href='https://carto.com/attributions'>CARTO</a>",
            "name": f"CartoDB {style_slug.title()}",
            "is_carto": True,
            "key": carto_key,
        }
    return {
        "tiles": "OpenStreetMap",
        "attr": "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors",
        "name": "OpenStreetMap",
        "is_carto": False,
        "key": "",
    }


def _build_station_popup_html(
    row: pd.Series,
    pollutant: str,
) -> str:
    """Construct an informative HTML popup card for a monitoring station marker."""
    st_name = html.escape(str(row.get("station_name") or row.get("station_id") or "Unknown Station"))
    st_id = html.escape(str(row.get("station_id", "N/A")))
    city = html.escape(str(row.get("city", "N/A")))
    state = html.escape(str(row.get("state", "N/A")))
    classification = str(row.get("classification", "unclassified"))
    label = get_classification_label(classification)
    color = get_classification_color(classification)
    unit = get_unit(pollutant)

    mean_conc = row.get("mean_concentration")
    max_conc = row.get("max_concentration")
    persistence = row.get("persistence_score")
    obs_count = row.get("observation_count") or row.get("valid_observations")
    threshold = get_threshold(pollutant)

    mean_str = f"{mean_conc:.1f} {unit}" if pd.notna(mean_conc) else "N/A"
    max_str = f"{max_conc:.1f} {unit}" if pd.notna(max_conc) else "N/A"
    persistence_str = f"{persistence:.1f}%" if pd.notna(persistence) else "N/A"
    obs_str = f"{int(obs_count):,}" if pd.notna(obs_count) else "N/A"

    # No default: if the column is missing we must not claim the position is official.
    coord_src = row.get("coord_source")
    if coord_src == "cpcb_registry":
        src_note = '<span style="color:#059669; font-size:10px;">Official CPCB Registry</span>'
    elif coord_src == "geocoded_locality":
        src_note = '<span style="color:#D97706; font-size:10px;">Approximate: nearby locality (geocoded)</span>'
    elif coord_src == "geocoded_city":
        src_note = '<span style="color:#D97706; font-size:10px;">Approximate: town centre only</span>'
    else:
        src_note = '<span style="color:#64748B; font-size:10px;">Coordinate source unknown</span>'

    html_content = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                font-size: 12px; width: 240px; padding: 4px; line-height: 1.4; color: #1E293B;">
        <div style="border-bottom: 2px solid {color}; padding-bottom: 4px; margin-bottom: 6px;">
            <div style="font-weight: 700; font-size: 13px; color: #0F172A;">{st_name}</div>
            <div style="font-size: 11px; color: #64748B;">{city}, {state} &bull; ID: {st_id}</div>
        </div>

        <div style="margin-bottom: 6px;">
            <span style="background-color: {color}22; color: {color}; border: 1px solid {color}88;
                         padding: 2px 6px; border-radius: 4px; font-weight: 600; font-size: 11px;">
                {label}
            </span>
        </div>

        <table style="width: 100%; border-collapse: collapse; font-size: 11px; margin-bottom: 6px;">
            <tr>
                <td style="color: #64748B; padding: 2px 0;">Mean Concentration:</td>
                <td style="text-align: right; font-weight: 600; color: #0F172A;">{mean_str}</td>
            </tr>
            <tr>
                <td style="color: #64748B; padding: 2px 0;">Observed Peak:</td>
                <td style="text-align: right; font-weight: 600; color: #DC2626;">{max_str}</td>
            </tr>
            <tr>
                <td style="color: #64748B; padding: 2px 0;">Persistence Score:</td>
                <td style="text-align: right; font-weight: 600; color: #0F172A;">{persistence_str}</td>
            </tr>
            <tr>
                <td style="color: #64748B; padding: 2px 0;">Hourly Observations:</td>
                <td style="text-align: right; font-weight: 600; color: #0F172A;">{obs_str}</td>
            </tr>
            <tr>
                <td style="color: #64748B; padding: 2px 0;">Threshold Baseline:</td>
                <td style="text-align: right; font-weight: 500; color: #475569;">{threshold} {unit}</td>
            </tr>
        </table>

        <div style="border-top: 1px solid #E2E8F0; padding-top: 4px; margin-top: 4px;">
            Provenance: {src_note}
        </div>
    </div>
    """
    return html_content


def _add_legend_html(m: folium.Map, pollutant: str) -> None:
    """Inject a floating HTML legend for hotspot classifications into the Folium map."""
    threshold = get_threshold(pollutant)
    unit = get_unit(pollutant)

    legend_items = []
    for key, color in CLASSIFICATION_COLORS.items():
        label = CLASSIFICATION_LABELS.get(key, key)
        legend_items.append(
            f'<div style="display:flex; align-items:center; margin-bottom:3px;">'
            f'<span style="display:inline-block; width:12px; height:12px; border-radius:50%; '
            f'background-color:{color}; margin-right:6px; border:1px solid #FFFFFF;"></span>'
            f'<span style="color:#334155; font-size:11px;">{label}</span>'
            f'</div>'
        )

    legend_html = f"""
    <div style="position: fixed;
                bottom: 25px; left: 25px; width: 220px; z-index: 9999;
                background-color: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(4px);
                border: 1px solid #CBD5E1;
                border-radius: 8px;
                padding: 10px 12px;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <div style="font-weight: 700; font-size: 12px; color: #0F172A; margin-bottom: 2px;">
            Hotspot Classification
        </div>
        <div style="font-size: 10px; color: #64748B; margin-bottom: 6px;">
            {pollutant} Threshold: {threshold} {unit}
        </div>
        {''.join(legend_items)}
        <div style="border-top: 1px solid #E2E8F0; margin-top: 6px; padding-top: 4px;
                    font-size: 9px; color: #94A3B8; line-height: 1.2;">
            Classifications represent concentration thresholds, distinct from regulatory AQI.
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))


def create_hotspot_map(
    spatial_df: pd.DataFrame,
    pollutant: str,
    selected_classifications: Sequence[str] | None = None,
    state: str | None = None,
    city: str | None = None,
    carto_api_key: str | None = None,
    basemap_style: str = "positron",
) -> folium.Map:
    """Create a Folium map showing monitoring stations styled by hotspot classification."""
    tile_cfg = _get_base_tiles(custom_key=carto_api_key, style=basemap_style)
    if spatial_df.empty:
        # Fallback empty map centered on India
        return folium.Map(
            location=DEFAULT_MAP_CENTER,
            zoom_start=DEFAULT_ZOOM_START,
            tiles=tile_cfg["tiles"],
            attr=tile_cfg.get("attr"),
        )

    # Filter by pollutant
    df = spatial_df[spatial_df["parameter"] == pollutant].copy()
    if df.empty:
        df = spatial_df.copy()

    # Filter by state and city if provided
    if state and state != "All States" and "state" in df.columns:
        df = df[df["state"] == state]
    if city and city != "All Cities" and "city" in df.columns:
        df = df[df["city"] == city]

    # Filter by selected classifications
    if selected_classifications and "classification" in df.columns:
        df = df[df["classification"].isin(selected_classifications)]

    # Compute optimal map center based on available coordinates
    valid_coords = df.dropna(subset=["latitude", "longitude"])
    if not valid_coords.empty:
        map_center = [valid_coords["latitude"].mean(), valid_coords["longitude"].mean()]
        zoom_start = 7 if (state and state != "All States") else DEFAULT_ZOOM_START
    else:
        map_center = DEFAULT_MAP_CENTER
        zoom_start = DEFAULT_ZOOM_START

    # Initialize Folium Map with clean base layer (OpenStreetMap default or CartoDB if key provided)
    m = folium.Map(
        location=map_center,
        zoom_start=zoom_start,
        tiles=tile_cfg["tiles"],
        attr=tile_cfg.get("attr"),
        control_scale=True,
    )

    if tile_cfg.get("is_carto"):
        folium.TileLayer("OpenStreetMap", name="OpenStreetMap Alternative").add_to(m)

    # Feature groups for each classification (enables layer control)
    feature_groups = {}
    for cls_key in CLASSIFICATION_COLORS.keys():
        fg = folium.FeatureGroup(name=CLASSIFICATION_LABELS.get(cls_key, cls_key), show=True)
        feature_groups[cls_key] = fg

    # Add markers
    for _, row in valid_coords.iterrows():
        lat = row["latitude"]
        lon = row["longitude"]
        cls = str(row.get("classification", "ordinary_fluctuation"))
        color = get_classification_color(cls)
        persistence = row.get("persistence_score", 0.0)

        # Dynamic marker size based on persistence (5 to 11 px radius)
        radius = 5 + (min(persistence, 100) / 100) * 6 if pd.notna(persistence) else 5

        popup_html = _build_station_popup_html(row, pollutant)
        popup = folium.Popup(popup_html, max_width=280)

        marker = folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            weight=1.5,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            popup=popup,
            tooltip=f"{row.get('station_name', row.get('station_id'))} ({get_classification_label(cls)})",
        )

        target_fg = feature_groups.get(cls)
        if target_fg is not None:
            marker.add_to(target_fg)
        else:
            marker.add_to(m)

    # Attach all feature groups to map
    for fg in feature_groups.values():
        fg.add_to(m)

    # Add Layer Control
    folium.LayerControl(position="topright", collapsed=True).add_to(m)

    # Add Legend
    _add_legend_html(m, pollutant)

    return m
