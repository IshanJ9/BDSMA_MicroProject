"""Data quality report: what was dropped and what the clean data looks like."""
import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

STAGES = [
    ("raw_rows", "Raw rows (all parameters)"),
    ("after_parameter_filter", "After keeping core pollutants"),
    ("after_null_drop", "After dropping null/NaN fields"),
    ("after_dedup", "After removing duplicates"),
    ("after_range_filter", "After removing negative / out-of-range values"),
    ("after_stuck_filter", "After removing stuck-sensor runs"),
    ("final_rows", "Final clean rows"),
]


def build_report(raw: DataFrame, clean: DataFrame, counts: dict) -> dict:
    span = clean.agg(F.min("timestamp").alias("start"), F.max("timestamp").alias("end")).first()
    n_stations = clean.select("station_id").distinct().count()
    no_coords = clean.filter(~F.col("has_coordinates")).select("station_id").distinct().count()

    per_param = (
        clean.groupBy("parameter")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("station_id").alias("stations"),
            F.round(F.mean("value"), 2).alias("mean"),
            F.round(F.expr("percentile_approx(value, 0.5)"), 2).alias("median"),
            F.round(F.max("value"), 2).alias("max"),
            F.first("unit").alias("unit"),
        )
        .orderBy("parameter")
        .collect()
    )
    mixed = clean.groupBy("parameter").agg(F.countDistinct("unit").alias("n_units")).filter("n_units > 1").collect()
    raw_nulls = raw.agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in ["station_id", "collected_at", "parameter_name", "value"]
        ]
    ).first()
    return {
        "counts": counts,
        "time_span": {"start": str(span["start"]), "end": str(span["end"])},
        "stations_total": n_stations,
        "stations_without_valid_coordinates": no_coords,
        "parameters": [r.asDict() for r in per_param],
        "parameters_with_mixed_units": [r["parameter"] for r in mixed],
        "raw_null_counts": raw_nulls.asDict(),
    }


def to_markdown(rep: dict) -> str:
    c = rep["counts"]
    raw = c["raw_rows"] or 1
    lines = [
        "# Data quality report",
        "",
        f"Period: {rep['time_span']['start']} to {rep['time_span']['end']} (IST)",
        "",
        "## Cleaning funnel",
        "",
        "| Stage | Rows | % of raw |",
        "|---|---:|---:|",
    ]
    for key, label in STAGES:
        if c.get(key) is not None:
            lines.append(f"| {label} | {c[key]:,} | {100 * c[key] / raw:.1f}% |")
    lines += [
        "",
        "## Coverage",
        "",
        f"- Stations in clean data: {rep['stations_total']}",
        f"- Stations without valid coordinates (excluded from maps): {rep['stations_without_valid_coordinates']}",
        f"- Parameters with mixed units: {rep['parameters_with_mixed_units'] or 'none'}",
        "",
        "## Per pollutant",
        "",
        "| Parameter | Unit | Rows | Stations | Mean | Median | Max |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for p in rep["parameters"]:
        lines.append(
            f"| {p['parameter']} | {p['unit']} | {p['rows']:,} | {p['stations']} | {p['mean']} | {p['median']} | {p['max']} |"
        )
    lines += ["", "## Raw null counts (before cleaning)", ""]
    lines += [f"- `{k}`: {v:,}" for k, v in rep["raw_null_counts"].items()]
    return "\n".join(lines) + "\n"


def write_report(rep: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "data_quality_report.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    (out_dir / "data_quality_report.md").write_text(to_markdown(rep), encoding="utf-8")
