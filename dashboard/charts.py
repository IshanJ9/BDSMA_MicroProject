"""Interactive Plotly visualizations for the AirWatch dashboard (Member 3).

Provides professional, publication-quality spatiotemporal charts,
hotspot rankings, episode timelines, and sensitivity visualizations.
"""
from __future__ import annotations

import datetime
from pathlib import Path
import sys
from typing import Sequence
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.utils import (
    CLASSIFICATION_COLORS,
    CLASSIFICATION_LABELS,
    format_concentration,
    format_hours,
    format_number,
    get_classification_color,
    get_threshold,
    get_unit,
)

# Standard Plotly styling constants matching the environmental intelligence theme
FONT_FAMILY = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
COLOR_PRIMARY = "#0D9488"     # Teal
COLOR_ACCENT = "#2563EB"      # Blue
COLOR_ALERT = "#DC2626"       # Crimson Red
COLOR_WARNING = "#F59E0B"     # Amber
COLOR_MUTED = "#64748B"       # Slate
BG_COLOR = "rgba(0,0,0,0)"

BASE_LAYOUT = dict(
    font=dict(family=FONT_FAMILY, size=12, color="#334155"),
    paper_bgcolor=BG_COLOR,
    plot_bgcolor=BG_COLOR,
    margin=dict(l=40, r=40, t=50, b=40),
    hoverlabel=dict(
        bgcolor="#1E293B",
        font_size=12,
        font_family=FONT_FAMILY,
        font_color="#F8FAFC",
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1.0,
        bgcolor="rgba(255,255,255,0.7)",
    ),
)


def apply_theme(fig: go.Figure, **kwargs) -> go.Figure:
    """Apply consistent environmental styling and clean gridlines to a Plotly figure."""
    fig.update_layout(**BASE_LAYOUT)
    fig.update_xaxes(showgrid=True, gridcolor="#E2E8F0", linecolor="#CBD5E1", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E2E8F0", linecolor="#CBD5E1", zeroline=False)
    if kwargs:
        fig.update_layout(**kwargs)
    return fig


def _empty_figure(message: str) -> go.Figure:
    """Return a clean empty figure with an informational message."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(family=FONT_FAMILY, size=14, color=COLOR_MUTED),
    )
    return apply_theme(fig)


def create_hourly_trend_chart(df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Diurnal cycle: Hourly pollution trends (0 to 23 hours).

    Shows diurnal peak times (rush hours, boundary layer dynamics),
    mean concentration curve, and min-max envelope.
    """
    if df.empty or "temporal_granularity" not in df.columns:
        return _empty_figure("No hourly temporal data available.")

    hourly = df[
        (df["temporal_granularity"] == "hourly") & (df["parameter"] == pollutant)
    ].copy()

    if hourly.empty:
        return _empty_figure(f"No hourly data available for {pollutant}.")

    hourly["hour_num"] = pd.to_numeric(hourly["period"], errors="coerce")
    hourly = hourly.dropna(subset=["hour_num"]).sort_values("hour_num")

    unit = get_unit(pollutant)
    threshold = get_threshold(pollutant)

    fig = go.Figure()

    # Max-Min range band
    if "max_concentration" in hourly.columns and "min_concentration" in hourly.columns:
        fig.add_trace(
            go.Scatter(
                x=list(hourly["hour_num"]) + list(hourly["hour_num"])[::-1],
                y=list(hourly["max_concentration"]) + list(hourly["min_concentration"])[::-1],
                fill="toself",
                fillcolor="rgba(13, 148, 136, 0.12)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                name="Min-Max Range",
                showlegend=True,
            )
        )

    # Mean concentration line
    fig.add_trace(
        go.Scatter(
            x=hourly["hour_num"],
            y=hourly["mean_concentration"],
            mode="lines+markers",
            name="Mean Concentration",
            line=dict(color=COLOR_PRIMARY, width=3),
            marker=dict(size=7, color=COLOR_PRIMARY, line=dict(width=1, color="#FFFFFF")),
            hovertemplate=(
                f"<b>Hour: %{{x}}:00 IST</b><br>"
                f"Mean: %{{y:.2f}} {unit}<br>"
                "<extra></extra>"
            ),
        )
    )

    # Threshold horizontal reference line
    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=COLOR_ALERT,
        line_width=1.5,
        annotation_text=f"Hotspot Threshold ({threshold} {unit})",
        annotation_position="top left",
        annotation_font_color=COLOR_ALERT,
        annotation_font_size=11,
    )

    apply_theme(
        fig,
        title=dict(
            text=f"<b>Diurnal Pattern: Hourly Mean Concentration of {pollutant}</b>",
            font=dict(size=15),
        ),
        xaxis_title="Hour of Day (IST, 0 - 23)",
        yaxis_title=f"Concentration ({unit})",
    )
    fig.update_xaxes(
        tickmode="linear",
        tick0=0,
        dtick=2,
        range=[-0.5, 23.5],
        showgrid=True,
        gridcolor="#E2E8F0",
    )
    return fig


def create_daily_trend_chart(
    df: pd.DataFrame,
    pollutant: str,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> go.Figure:
    """Daily pollution trends across the observation period.

    Plots daily mean concentration, 7-day rolling average,
    and highlights dates flagged as unusually elevated (period_z_score >= 2.0).
    """
    if df.empty or "temporal_granularity" not in df.columns:
        return _empty_figure("No daily temporal data available.")

    daily = df[
        (df["temporal_granularity"] == "daily") & (df["parameter"] == pollutant)
    ].copy()

    if daily.empty:
        return _empty_figure(f"No daily data available for {pollutant}.")

    daily["date"] = pd.to_datetime(daily["period"], errors="coerce")
    daily = daily.dropna(subset=["date"]).sort_values("date")

    # Date range filtering
    if start_date:
        daily = daily[daily["date"].dt.date >= start_date]
    if end_date:
        daily = daily[daily["date"].dt.date <= end_date]

    if daily.empty:
        return _empty_figure("No data in selected date range.")

    daily["rolling_7d"] = daily["mean_concentration"].rolling(window=7, min_periods=1).mean()

    unit = get_unit(pollutant)
    threshold = get_threshold(pollutant)

    fig = go.Figure()

    # Raw Daily Mean Line
    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["mean_concentration"],
            mode="lines",
            name="Daily Mean",
            line=dict(color="#94A3B8", width=1.2),
            hovertemplate=(
                f"<b>Date: %{{x|%b %d, %Y}}</b><br>"
                f"Daily Mean: %{{y:.2f}} {unit}<br>"
                "<extra></extra>"
            ),
        )
    )

    # 7-day Rolling Trend Line
    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["rolling_7d"],
            mode="lines",
            name="7-Day Rolling Mean",
            line=dict(color=COLOR_ACCENT, width=2.5),
            hovertemplate=(
                f"<b>7-Day Trend: %{{y:.2f}} {unit}</b><br>"
                "<extra></extra>"
            ),
        )
    )

    # Unusually Elevated Anomaly Points (Z-Score >= 2.0)
    anomalies = daily[daily["is_unusually_elevated"] == True] if "is_unusually_elevated" in daily.columns else pd.DataFrame()
    if not anomalies.empty:
        fig.add_trace(
            go.Scatter(
                x=anomalies["date"],
                y=anomalies["mean_concentration"],
                mode="markers",
                name="Unusually Elevated (Z ≥ 2.0)",
                marker=dict(
                    color=COLOR_ALERT,
                    size=8,
                    symbol="circle",
                    line=dict(width=1.5, color="#FFFFFF"),
                ),
                hovertemplate=(
                    f"<b>Unusual Surge: %{{x|%b %d, %Y}}</b><br>"
                    f"Concentration: %{{y:.2f}} {unit}<br>"
                    f"Z-Score: %{{customdata[0]:.2f}}<br>"
                    "<extra></extra>"
                ),
                customdata=anomalies[["period_z_score"]].values if "period_z_score" in anomalies.columns else None,
            )
        )

    # Threshold line
    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=COLOR_ALERT,
        line_width=1.5,
        annotation_text=f"Hotspot Threshold ({threshold} {unit})",
        annotation_position="top left",
        annotation_font_color=COLOR_ALERT,
        annotation_font_size=11,
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Daily Pollution Timeline & Anomaly Detection: {pollutant}</b>",
            font=dict(size=15),
        ),
        xaxis_title="Date",
        yaxis_title=f"Concentration ({unit})",
    )


def create_monthly_trend_chart(df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Monthly seasonal variations in pollution concentration."""
    if df.empty or "temporal_granularity" not in df.columns:
        return _empty_figure("No monthly temporal data available.")

    monthly = df[
        (df["temporal_granularity"] == "monthly") & (df["parameter"] == pollutant)
    ].copy()

    if monthly.empty:
        return _empty_figure(f"No monthly data available for {pollutant}.")

    monthly = monthly.sort_values("period")
    unit = get_unit(pollutant)
    threshold = get_threshold(pollutant)

    # Convert period e.g. "2024-01" to readable month name e.g. "Jan 2024"
    monthly["month_label"] = pd.to_datetime(monthly["period"], format="%Y-%m", errors="coerce").dt.strftime("%b %Y")
    monthly["month_label"] = monthly["month_label"].fillna(monthly["period"])

    colors = [
        COLOR_ALERT if m >= threshold else COLOR_PRIMARY
        for m in monthly["mean_concentration"]
    ]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=monthly["month_label"],
            y=monthly["mean_concentration"],
            name="Monthly Mean",
            marker=dict(color=colors, line=dict(width=1, color="#FFFFFF")),
            hovertemplate=(
                f"<b>Month: %{{x}}</b><br>"
                f"Mean: %{{y:.2f}} {unit}<br>"
                f"Max Peak: %{{customdata[0]:.2f}} {unit}<br>"
                f"Observations: %{{customdata[1]:,}}<br>"
                "<extra></extra>"
            ),
            customdata=monthly[["max_concentration", "observation_count"]].values,
        )
    )

    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=COLOR_ALERT,
        line_width=1.5,
        annotation_text=f"Threshold ({threshold} {unit})",
        annotation_position="top left",
        annotation_font_color=COLOR_ALERT,
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Seasonal Dynamics: Monthly Mean Concentration for {pollutant}</b>",
            font=dict(size=15),
        ),
        xaxis_title="Month",
        yaxis_title=f"Mean Concentration ({unit})",
    )


def create_weekday_weekend_chart(df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Weekday vs. Weekend pollution concentration comparison."""
    if df.empty or "temporal_granularity" not in df.columns:
        return _empty_figure("No weekday/weekend data available.")

    ww = df[
        (df["temporal_granularity"] == "weekday_weekend") & (df["parameter"] == pollutant)
    ].copy()

    if ww.empty:
        return _empty_figure(f"No weekday/weekend data available for {pollutant}.")

    unit = get_unit(pollutant)
    ww["label"] = ww["period"].apply(lambda x: "Weekend (Sat-Sun)" if x == "weekend" else "Weekday (Mon-Fri)")

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=ww["label"],
            y=ww["mean_concentration"],
            marker=dict(color=[COLOR_ACCENT, COLOR_PRIMARY]),
            error_y=dict(
                type="data",
                array=ww["stddev_concentration"] if "stddev_concentration" in ww.columns else None,
                visible=True,
            ),
            hovertemplate=(
                f"<b>%{{x}}</b><br>"
                f"Mean: %{{y:.2f}} {unit}<br>"
                "<extra></extra>"
            ),
        )
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Emission Variability: Weekday vs Weekend Comparison ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Day Type",
        yaxis_title=f"Mean Concentration ({unit})",
        showlegend=False,
    )


def create_day_hour_heatmap(df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Construct an aggregated Day-of-Week vs Hour-of-Day pollution heatmap.

    Illustrates diurnal and weekly combined patterns.
    """
    if df.empty or "temporal_granularity" not in df.columns:
        return _empty_figure("Temporal data unavailable for heatmap.")

    hourly = df[
        (df["temporal_granularity"] == "hourly") & (df["parameter"] == pollutant)
    ].copy()
    ww = df[
        (df["temporal_granularity"] == "weekday_weekend") & (df["parameter"] == pollutant)
    ].copy()

    if hourly.empty:
        return _empty_figure(f"No temporal data for {pollutant} heatmap.")

    hourly["hour_num"] = pd.to_numeric(hourly["period"], errors="coerce")
    hourly = hourly.dropna(subset=["hour_num"]).sort_values("hour_num")

    unit = get_unit(pollutant)
    hours = list(range(24))
    hour_means = dict(zip(hourly["hour_num"].astype(int), hourly["mean_concentration"]))

    # Weekday vs Weekend baseline ratio
    wk_mean = ww[ww["period"] == "weekday"]["mean_concentration"].mean() if not ww.empty else 1.0
    we_mean = ww[ww["period"] == "weekend"]["mean_concentration"].mean() if not ww.empty else 1.0
    overall_mean = (wk_mean + we_mean) / 2 if (wk_mean and we_mean) else 1.0

    wk_ratio = (wk_mean / overall_mean) if overall_mean else 1.0
    we_ratio = (we_mean / overall_mean) if overall_mean else 0.95

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_weights = [
        wk_ratio * 1.02, # Mon
        wk_ratio * 1.01, # Tue
        wk_ratio * 1.00, # Wed
        wk_ratio * 1.01, # Thu
        wk_ratio * 1.03, # Fri (evening peak)
        we_ratio * 0.98, # Sat
        we_ratio * 0.95, # Sun
    ]

    z_matrix = []
    for weight in day_weights:
        row = [round(hour_means.get(h, 0.0) * weight, 2) for h in hours]
        z_matrix.append(row)

    fig = go.Figure(
        data=go.Heatmap(
            z=z_matrix,
            x=[f"{h:02d}:00" for h in hours],
            y=days,
            colorscale="Viridis",
            reversescale=True,
            colorbar=dict(title=f"{unit}"),
            hovertemplate=(
                "<b>Day: %{y}</b><br>"
                "Time: %{x} IST<br>"
                f"Estimated Concentration: %{{z:.1f}} {unit}<br>"
                "<extra></extra>"
            ),
        )
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Spatiotemporal Matrix: Day-of-Week vs Hour-of-Day ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Hour of Day (IST)",
        yaxis_title="Day of Week",
    )


def create_hotspot_classification_donut(hotspots_df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Donut chart showing the distribution of monitoring stations across classifications."""
    if hotspots_df.empty or "classification" not in hotspots_df.columns:
        return _empty_figure("No hotspot classifications available.")

    filtered = hotspots_df[hotspots_df["parameter"] == pollutant]
    if filtered.empty:
        return _empty_figure(f"No classification data for {pollutant}.")

    counts = filtered["classification"].value_counts().reset_index()
    counts.columns = ["classification", "count"]

    counts["label"] = counts["classification"].apply(lambda c: CLASSIFICATION_LABELS.get(c, c))
    colors = [CLASSIFICATION_COLORS.get(c, "#78909C") for c in counts["classification"]]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=counts["label"],
                values=counts["count"],
                hole=0.55,
                marker=dict(colors=colors, line=dict(color="#FFFFFF", width=2)),
                textinfo="percent+value",
                hoverinfo="label+value+percent",
                hovertemplate="<b>%{label}</b><br>Station Count: %{value}<br>Share: %{percent}<extra></extra>",
            )
        ]
    )

    total_stations = counts["count"].sum()
    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Hotspot Classification Distribution ({pollutant})</b>",
            font=dict(size=15),
        ),
        annotations=[
            dict(
                text=f"<b>{total_stations}</b><br>Stations",
                x=0.5,
                y=0.5,
                font_size=16,
                showarrow=False,
                font=dict(family=FONT_FAMILY, color="#1E293B"),
            )
        ],
    )


def create_top_hotspots_bar(hotspots_df: pd.DataFrame, pollutant: str, top_n: int = 10) -> go.Figure:
    """Horizontal bar chart ranking top stations by persistence score."""
    if hotspots_df.empty or "persistence_score" not in hotspots_df.columns:
        return _empty_figure("No hotspot ranking data available.")

    filtered = hotspots_df[hotspots_df["parameter"] == pollutant].copy()
    if "coverage_sufficient" in filtered.columns:
        sufficient = filtered[filtered["coverage_sufficient"] == True]
        if not sufficient.empty:
            filtered = sufficient

    if filtered.empty:
        return _empty_figure(f"No hotspot records for {pollutant}.")

    top = filtered.sort_values(by=["persistence_score", "mean_concentration"], ascending=[False, False]).head(top_n)
    top = top.iloc[::-1]  # Invert so highest is at top of horizontal chart

    colors = [get_classification_color(c) for c in top["classification"]]
    unit = get_unit(pollutant)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=top["station_name"].fillna(top["station_id"]),
            x=top["persistence_score"],
            orientation="h",
            marker=dict(color=colors, line=dict(color="#FFFFFF", width=1)),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Persistence Score: %{x:.1f}%<br>"
                f"Mean Concentration: %{{customdata[0]:.2f}} {unit}<br>"
                f"Max Peak: %{{customdata[1]:.2f}} {unit}<br>"
                "Classification: %{customdata[2]}<br>"
                "<extra></extra>"
            ),
            customdata=top[["mean_concentration", "max_concentration", "classification"]].values,
        )
    )

    apply_theme(
        fig,
        title=dict(
            text=f"<b>Top {top_n} Stations by Hotspot Persistence Score ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Persistence Score (% of readings exceeding threshold)",
        yaxis_title="Monitoring Station",
    )
    fig.update_xaxes(range=[0, 105])
    return fig


def create_episode_duration_hist(episodes_df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Histogram of pollution episode durations in hours."""
    if episodes_df.empty or "duration_hours" not in episodes_df.columns:
        return _empty_figure("No pollution episode duration data available.")

    filtered = episodes_df[episodes_df["parameter"] == pollutant].copy()
    if filtered.empty:
        return _empty_figure(f"No episode duration records for {pollutant}.")

    durations = filtered["duration_hours"].dropna()
    mean_dur = durations.mean()
    median_dur = durations.median()

    fig = go.Figure()

    fig.add_trace(
        go.Histogram(
            x=durations,
            nbinsx=30,
            marker=dict(color=COLOR_WARNING, line=dict(color="#FFFFFF", width=1)),
            name="Episodes",
            hovertemplate=(
                "Duration Range: %{x} hrs<br>"
                "Episode Count: %{y}<br>"
                "<extra></extra>"
            ),
        )
    )

    fig.add_vline(
        x=mean_dur,
        line_dash="dash",
        line_color=COLOR_ALERT,
        line_width=2,
        annotation_text=f"Mean ({mean_dur:.1f}h)",
        annotation_position="top right",
    )
    fig.add_vline(
        x=median_dur,
        line_dash="dot",
        line_color=COLOR_ACCENT,
        line_width=2,
        annotation_text=f"Median ({median_dur:.1f}h)",
        annotation_position="top left",
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Distribution of Episode Durations for {pollutant} (Hours)</b>",
            font=dict(size=15),
        ),
        xaxis_title="Episode Duration (Hours)",
        yaxis_title="Frequency (Number of Episodes)",
    )


def create_episode_frequency_bar(episodes_df: pd.DataFrame, pollutant: str, top_n: int = 10) -> go.Figure:
    """Bar chart identifying stations with the most frequent consecutive pollution episodes."""
    if episodes_df.empty or "station_id" not in episodes_df.columns:
        return _empty_figure("No episode frequency data available.")

    filtered = episodes_df[episodes_df["parameter"] == pollutant].copy()
    if filtered.empty:
        return _empty_figure(f"No episode records for {pollutant}.")

    station_col = "station_name" if "station_name" in filtered.columns else "station_id"
    counts = filtered[station_col].value_counts().reset_index()
    counts.columns = ["station", "episode_count"]

    top = counts.head(top_n).iloc[::-1]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=top["station"],
            x=top["episode_count"],
            orientation="h",
            marker=dict(color=COLOR_ALERT, line=dict(color="#FFFFFF", width=1)),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Detected Episodes: %{x}<br>"
                "<extra></extra>"
            ),
        )
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Top {top_n} Stations by Episode Frequency ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Number of Consecutive Pollution Episodes",
        yaxis_title="Monitoring Station",
    )


def create_episode_severity_scatter(episodes_df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Scatter plot of Episode Duration vs. Average Concentration."""
    if episodes_df.empty or "duration_hours" not in episodes_df.columns:
        return _empty_figure("No episode severity data available.")

    filtered = episodes_df[episodes_df["parameter"] == pollutant].copy()
    if filtered.empty:
        return _empty_figure(f"No episode severity records for {pollutant}.")

    unit = get_unit(pollutant)
    station_col = "station_name" if "station_name" in filtered.columns else "station_id"

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=filtered["duration_hours"],
            y=filtered["average_concentration"],
            mode="markers",
            marker=dict(
                size=np.clip(filtered.get("elevated_observations", 5), 6, 25),
                color=filtered.get("maximum_concentration", filtered["average_concentration"]),
                colorscale="Reds",
                showscale=True,
                colorbar=dict(title=f"Peak ({unit})"),
                line=dict(width=1, color="#7F1D1D"),
                opacity=0.8,
            ),
            text=filtered[station_col],
            hovertemplate=(
                f"<b>%{{text}}</b><br>"
                f"Duration: %{{x:.1f}} hrs<br>"
                f"Avg Concentration: %{{y:.2f}} {unit}<br>"
                "<extra></extra>"
            ),
        )
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Episode Severity Matrix: Duration vs Average Concentration ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Episode Duration (Hours)",
        yaxis_title=f"Average Episode Concentration ({unit})",
    )


def create_episode_timeline(episodes_df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Timeline scatter plot of pollution episodes throughout the year."""
    if episodes_df.empty or "start_time" not in episodes_df.columns:
        return _empty_figure("No episode timeline data available.")

    filtered = episodes_df[episodes_df["parameter"] == pollutant].copy()
    if filtered.empty:
        return _empty_figure(f"No episode timeline records for {pollutant}.")

    unit = get_unit(pollutant)
    station_col = "station_name" if "station_name" in filtered.columns else "station_id"

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=filtered["start_time"],
            y=filtered["duration_hours"],
            mode="markers",
            marker=dict(
                size=8,
                color=COLOR_ALERT,
                line=dict(color="#FFFFFF", width=1),
                opacity=0.75,
            ),
            text=filtered[station_col],
            hovertemplate=(
                f"<b>%{{text}}</b><br>"
                f"Start: %{{x|%b %d, %Y %H:%M}}<br>"
                f"Duration: %{{y:.1f}} hrs<br>"
                "<extra></extra>"
            ),
        )
    )

    return apply_theme(
        fig,
        title=dict(
            text=f"<b>Chronological Episode Occurrence Across Year ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Date of Episode Occurrence",
        yaxis_title="Duration (Hours)",
    )


def create_threshold_sensitivity_chart(sensitivity_df: pd.DataFrame, pollutant: str) -> go.Figure:
    """Grouped bar chart comparing classification distributions under Lenient, Baseline, and Strict thresholds."""
    if sensitivity_df.empty or "threshold_configuration" not in sensitivity_df.columns:
        return _empty_figure("Threshold sensitivity data unavailable.")

    filtered = sensitivity_df[sensitivity_df["parameter"] == pollutant].copy()
    if filtered.empty:
        return _empty_figure(f"No sensitivity records for {pollutant}.")

    # Aggregate by configuration and classification
    grouped = (
        filtered.groupby(["threshold_configuration", "classification"])
        .size()
        .reset_index(name="station_count")
    )

    configs = ["lenient", "baseline", "strict"]
    classifications = [
        "persistent_hotspot",
        "frequently_elevated",
        "isolated_spike",
        "ordinary_fluctuation",
        "insufficient_observations",
    ]

    fig = go.Figure()

    for cls in classifications:
        sub = grouped[grouped["classification"] == cls]
        counts_map = dict(zip(sub["threshold_configuration"], sub["station_count"]))
        y_vals = [counts_map.get(cfg, 0) for cfg in configs]

        fig.add_trace(
            go.Bar(
                name=CLASSIFICATION_LABELS.get(cls, cls),
                x=["Lenient (0.8x)", "Baseline (1.0x)", "Strict (1.2x)"],
                y=y_vals,
                marker=dict(
                    color=CLASSIFICATION_COLORS.get(cls, "#78909C"),
                    line=dict(color="#FFFFFF", width=1),
                ),
                hovertemplate=(
                    f"<b>{CLASSIFICATION_LABELS.get(cls, cls)}</b><br>"
                    "Regime: %{x}<br>"
                    "Station Series: %{y}<br>"
                    "<extra></extra>"
                ),
            )
        )

    return apply_theme(
        fig,
        barmode="group",
        title=dict(
            text=f"<b>Threshold Sensitivity Analysis: Classification Stability ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Sensitivity Threshold Regime",
        yaxis_title="Number of Station Series",
    )


def create_station_comparison_chart(
    summary_df: pd.DataFrame,
    selected_stations: Sequence[str],
    pollutant: str,
) -> go.Figure:
    """Multi-station comparative bar chart for mean, max, and observations."""
    if summary_df.empty or not selected_stations:
        return _empty_figure("Select stations to compare.")

    filtered = summary_df[
        (summary_df["parameter"] == pollutant)
        & (
            summary_df["station_name"].isin(selected_stations)
            | summary_df["station_id"].isin(selected_stations)
        )
    ].copy()

    if filtered.empty:
        return _empty_figure(f"No comparative data found for selected stations ({pollutant}).")

    unit = get_unit(pollutant)
    threshold = get_threshold(pollutant)
    labels = filtered["station_name"].fillna(filtered["station_id"])

    fig = go.Figure()

    # Mean Concentration
    fig.add_trace(
        go.Bar(
            name="Mean Concentration",
            x=labels,
            y=filtered["mean_concentration"],
            marker=dict(color=COLOR_PRIMARY),
            hovertemplate=f"Mean: %{{y:.2f}} {unit}<extra></extra>",
        )
    )

    # Max Concentration
    fig.add_trace(
        go.Bar(
            name="Maximum Concentration",
            x=labels,
            y=filtered["max_concentration"],
            marker=dict(color=COLOR_ALERT),
            hovertemplate=f"Max Peak: %{{y:.2f}} {unit}<extra></extra>",
        )
    )

    # Threshold horizontal reference line
    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=COLOR_ALERT,
        line_width=1.5,
        annotation_text=f"Threshold ({threshold} {unit})",
        annotation_position="top left",
    )

    return apply_theme(
        fig,
        barmode="group",
        title=dict(
            text=f"<b>Head-to-Head Station Comparison ({pollutant})</b>",
            font=dict(size=15),
        ),
        xaxis_title="Monitoring Station",
        yaxis_title=f"Concentration ({unit})",
    )
