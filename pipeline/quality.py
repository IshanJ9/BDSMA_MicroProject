"""Data quality report: what was dropped, how complete the data is, and how long processing took."""
import csv
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

# Share of the period's hours a station-pollutant series must cover to count as "usable".
USABLE_COMPLETENESS = 0.75
BUCKETS = [("90%+", 0.90), ("75-90%", 0.75), ("50-75%", 0.50), ("below 50%", 0.0)]


def completeness(clean: DataFrame) -> DataFrame:
    """Per station and pollutant: hours with a valid reading / hours in the whole period.

    Readings removed by cleaning (out of range, stuck sensor) count as missing. Missing readings are
    absent rows in the source, not nulls, so this is the meaningful missing-data measure.
    """
    span = clean.agg(F.min("date").alias("d0"), F.max("date").alias("d1")).first()
    period_hours = ((span["d1"] - span["d0"]).days + 1) * 24
    return (
        clean.groupBy("station_id", "station_name", "city", "state", "coord_source", "parameter")
        .agg(F.countDistinct("date", "hour").alias("hours_with_data"))
        .withColumn("period_hours", F.lit(period_hours))
        .withColumn("completeness", F.round(F.col("hours_with_data") / F.lit(period_hours), 4))
    )


def _bucket(c: float) -> str:
    return next(name for name, lo in BUCKETS if c >= lo)


def build_report(raw: DataFrame, clean: DataFrame, counts: dict, perf: dict | None = None) -> tuple[dict, list]:
    """Return (report dict, per station-pollutant completeness rows)."""
    span = clean.agg(F.min("timestamp").alias("start"), F.max("timestamp").alias("end")).first()
    n_stations = clean.select("station_id").distinct().count()
    no_coords = clean.filter(~F.col("has_coordinates")).select("station_id").distinct().count()
    by_source = {
        (r["coord_source"] or "none"): r["n"]
        for r in clean.select("station_id", "coord_source")
        .distinct()
        .groupBy("coord_source")
        .agg(F.count("*").alias("n"))
        .collect()
    }

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

    comp_rows = [r.asDict() for r in completeness(clean).orderBy("station_id", "parameter").collect()]
    comp = {}
    for r in comp_rows:
        p = comp.setdefault(r["parameter"], {"series": 0, "usable": 0, "buckets": {b: 0 for b, _ in BUCKETS}, "vals": []})
        p["series"] += 1
        p["usable"] += r["completeness"] >= USABLE_COMPLETENESS
        p["buckets"][_bucket(r["completeness"])] += 1
        p["vals"].append(r["completeness"])
    completeness_summary = {}
    for param, p in sorted(comp.items()):
        vals = sorted(p.pop("vals"))
        p["median_completeness"] = vals[len(vals) // 2]
        completeness_summary[param] = p

    report = {
        "counts": counts,
        "time_span": {"start": str(span["start"]), "end": str(span["end"])},
        "period_hours": comp_rows[0]["period_hours"] if comp_rows else 0,
        "stations_total": n_stations,
        "stations_without_valid_coordinates": no_coords,
        "stations_by_coordinate_source": by_source,
        "parameters": [r.asDict() for r in per_param],
        "parameters_with_mixed_units": [r["parameter"] for r in mixed],
        "raw_null_counts": raw_nulls.asDict(),
        "completeness": completeness_summary,
        "performance": perf or {},
    }
    return report, comp_rows


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
        "- Stations by coordinate source: "
        + ", ".join(f"{k} {v}" for k, v in sorted(rep["stations_by_coordinate_source"].items())),
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

    lines += [
        "",
        "## Missing data (completeness)",
        "",
        f"Completeness = hours with a valid reading / {rep['period_hours']:,} hours in the period. Missing readings are "
        "absent rows in the source rather than nulls, and readings removed by cleaning count as missing. "
        f"A series is *usable* at {USABLE_COMPLETENESS:.0%} or more. Per-series values: "
        "`reports/station_completeness.csv`.",
        "",
        "| Parameter | Station series | Usable | Median completeness | " + " | ".join(b for b, _ in BUCKETS) + " |",
        "|---|---:|---:|---:|" + "---:|" * len(BUCKETS),
    ]
    for param, p in rep["completeness"].items():
        lines.append(
            f"| {param} | {p['series']} | {p['usable']} ({p['usable'] / p['series']:.0%}) | "
            f"{p['median_completeness']:.0%} | " + " | ".join(str(p["buckets"][b]) for b, _ in BUCKETS) + " |"
        )

    perf = rep.get("performance") or {}
    if perf:
        lines += [
            "",
            "## Processing performance",
            "",
            f"- Input: {perf['input_files']} Parquet files, {perf['input_mb']:.0f} MB on disk, {c['raw_rows']:,} rows",
            f"- Cleaning, funnel counts and write: {perf['clean_write_seconds']:.0f} s "
            f"({c['raw_rows'] / perf['clean_write_seconds']:,.0f} raw rows/s)",
            f"- Quality report (aggregations over the clean data): {perf['report_seconds']:.0f} s",
            f"- Spark: local mode, {perf['cores']} cores, driver memory {perf['driver_memory']}, "
            f"{perf['shuffle_partitions']} shuffle partitions",
            "- The funnel counts run extra Spark jobs over the data, so a run without them would be faster.",
        ]

    lines += ["", "## Raw null counts (before cleaning)", ""]
    lines += [f"- `{k}`: {v:,}" for k, v in rep["raw_null_counts"].items()]
    lines.append("- All zero: the source leaves gaps as absent rows, so see the completeness table above.")
    return "\n".join(lines) + "\n"


def write_report(rep: dict, comp_rows: list, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "data_quality_report.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    (out_dir / "data_quality_report.md").write_text(to_markdown(rep), encoding="utf-8")
    cols = ["station_id", "station_name", "city", "state", "coord_source", "parameter",
            "hours_with_data", "period_hours", "completeness"]
    with open(out_dir / "station_completeness.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(comp_rows)
