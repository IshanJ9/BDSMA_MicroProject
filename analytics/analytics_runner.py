"""Distributed spatiotemporal analytics for the cleaned AirWatch measurements.

The input is the long-form contract produced by ``pipeline.clean``.  Concentration
thresholds are pollutant-specific and are deliberately kept separate from AQI
breakpoints: this module reports measured concentration, not AQI.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from pipeline import config
from pipeline.spark_session import get_spark

DEFAULT_THRESHOLDS = {
    "PM2.5": 60.0,
    "PM10": 100.0,
    "NO2": 80.0,
    "SO2": 80.0,
    "CO": 4.0,
    "Ozone": 100.0,
    "NH3": 400.0,
}

REQUIRED_COLUMNS = {"station_id", "parameter", "value", "timestamp"}


def _validate_input(df: DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Clean measurement data is missing columns: {sorted(missing)}")


def prepare_measurements(df: DataFrame) -> DataFrame:
    """Keep valid observations and add shared temporal and coordinate features."""
    _validate_input(df)
    d = (
        df.withColumn("_value", F.col("value").cast("double"))
        .withColumn("_timestamp", F.col("timestamp").cast("timestamp"))
        .filter(
            F.col("station_id").isNotNull()
            & F.col("parameter").isNotNull()
            & F.col("_value").isNotNull()
            & ~F.isnan("_value")
            & F.col("_timestamp").isNotNull()
        )
        .withColumn("date", F.to_date("_timestamp"))
        .withColumn("hour", F.hour("_timestamp"))
        .withColumn("month", F.date_format("_timestamp", "yyyy-MM"))
        .withColumn("day_of_week", F.dayofweek("_timestamp"))
        .withColumn("is_weekend", F.dayofweek("_timestamp").isin(1, 7))
    )
    if "latitude" in d.columns and "longitude" in d.columns:
        d = d.withColumn(
            "coordinate_valid",
            F.col("latitude").between(*config.INDIA_LAT)
            & F.col("longitude").between(*config.INDIA_LON),
        )
    else:
        d = d.withColumn("coordinate_valid", F.lit(False))
    return d


def _stats(group_columns: list[str]) -> list:
    return [
        F.round(F.mean("_value"), 4).alias("mean_concentration"),
        F.round(F.max("_value"), 4).alias("max_concentration"),
        F.round(F.min("_value"), 4).alias("min_concentration"),
        F.round(F.stddev("_value"), 4).alias("stddev_concentration"),
        F.count("_value").alias("observation_count"),
    ]


def temporal_analytics(df: DataFrame) -> DataFrame:
    """Return one tidy table containing hourly, daily, monthly and weekday views."""
    d = prepare_measurements(df)
    views = [
        d.groupBy("parameter", "hour")
        .agg(*_stats(["parameter", "hour"]))
        .withColumn("temporal_granularity", F.lit("hourly"))
        .withColumn("period", F.col("hour").cast("string")),
        d.groupBy("parameter", "date")
        .agg(*_stats(["parameter", "date"]))
        .withColumn("temporal_granularity", F.lit("daily"))
        .withColumn("period", F.col("date").cast("string"))
        .drop("date"),
        d.groupBy("parameter", "month")
        .agg(*_stats(["parameter", "month"]))
        .withColumn("temporal_granularity", F.lit("monthly"))
        .withColumn("period", F.col("month")),
        d.groupBy("parameter", "is_weekend")
        .agg(*_stats(["parameter", "is_weekend"]))
        .withColumn("temporal_granularity", F.lit("weekday_weekend"))
        .withColumn("period", F.when(F.col("is_weekend"), F.lit("weekend")).otherwise(F.lit("weekday")))
        .drop("is_weekend"),
    ]
    result = views[0].unionByName(views[1], allowMissingColumns=True).unionByName(
        views[2], allowMissingColumns=True
    ).unionByName(views[3], allowMissingColumns=True).select(
        "temporal_granularity", "parameter", "period",
        "mean_concentration", "max_concentration", "min_concentration",
        "stddev_concentration", "observation_count",
    )
    period_window = Window.partitionBy("temporal_granularity", "parameter")
    return (
        result.withColumn("_period_mean", F.avg("mean_concentration").over(period_window))
        .withColumn("_period_stddev", F.stddev("mean_concentration").over(period_window))
        .withColumn(
            "period_z_score",
            F.round(
                (F.col("mean_concentration") - F.col("_period_mean"))
                / F.when(F.col("_period_stddev") > 0, F.col("_period_stddev")),
                3,
            ),
        )
        .withColumn(
            "is_unusually_elevated",
            F.coalesce(F.col("period_z_score") >= 2.0, F.lit(False)),
        )
        .drop("_period_mean", "_period_stddev")
    )


def station_summary(df: DataFrame) -> DataFrame:
    """Summarize every station/pollutant and retain coverage evidence."""
    d = prepare_measurements(df)
    columns = [c for c in ["station_id", "station_name", "city", "state", "parameter"] if c in d.columns]
    return d.groupBy(*columns).agg(
        *_stats(columns),
        F.min("_timestamp").alias("first_observation"),
        F.max("_timestamp").alias("last_observation"),
        F.countDistinct("date").alias("active_days"),
    )


def station_rankings(summary: DataFrame) -> DataFrame:
    """Rank stations within each pollutant, with count as a visible reliability signal."""
    w = Window.partitionBy("parameter").orderBy(
        F.col("mean_concentration").desc_nulls_last(),
        F.col("observation_count").desc(),
        F.col("station_id"),
    )
    return summary.withColumn("mean_rank", F.row_number().over(w))


def _threshold_column(thresholds: Mapping[str, float]):
    return F.create_map(*[x for p, t in thresholds.items() for x in (F.lit(p), F.lit(float(t)))])


def hotspot_classifications(
    df: DataFrame,
    thresholds: Mapping[str, float] | None = None,
    min_observations: int = 24,
    frequent_persistence: float = 0.25,
    persistent_persistence: float = 0.50,
    magnitude_multiplier: float = 1.5,
) -> DataFrame:
    """Classify station/pollutant series using magnitude, persistence and coverage.

    ``persistence_score`` is the percentage of valid observations above that
    pollutant's threshold. It is not used alone: a persistent hotspot also
    needs adequate observations, an elevated mean, and a high observed maximum.
    """
    thresholds = thresholds or DEFAULT_THRESHOLDS
    d = prepare_measurements(df).withColumn("_threshold", _threshold_column(thresholds)[F.col("parameter")])
    keys = [c for c in ["station_id", "station_name", "city", "state", "parameter"] if c in d.columns]
    result = d.groupBy(*keys).agg(
        F.count("_value").alias("valid_observations"),
        F.sum(F.when(F.col("_value") >= F.col("_threshold"), 1).otherwise(0)).alias("elevated_observations"),
        F.round(F.mean("_value"), 4).alias("mean_concentration"),
        F.round(F.max("_value"), 4).alias("max_concentration"),
        F.first("_threshold").alias("threshold"),
    ).withColumn(
        "persistence_score",
        F.round(F.col("elevated_observations") / F.col("valid_observations") * 100, 2),
    ).withColumn(
        "coverage_sufficient", F.col("valid_observations") >= F.lit(min_observations)
    ).withColumn(
        "classification",
        F.when(~F.col("coverage_sufficient"), F.lit("insufficient_observations"))
        .when(
            (F.col("persistence_score") >= persistent_persistence * 100)
            & (F.col("mean_concentration") >= F.col("threshold"))
            & (F.col("max_concentration") >= F.col("threshold") * magnitude_multiplier),
            F.lit("persistent_hotspot"),
        )
        .when(
            F.col("persistence_score") >= frequent_persistence * 100,
            F.lit("frequently_elevated"),
        )
        .when(F.col("max_concentration") >= F.col("threshold"), F.lit("isolated_spike"))
        .otherwise(F.lit("ordinary_fluctuation")),
    )
    return result


def threshold_sensitivity(
    df: DataFrame,
    thresholds: Mapping[str, float] | None = None,
    min_observations: int = 24,
) -> DataFrame:
    """Compare lenient, baseline and strict thresholds without rescanning per configuration."""
    base = thresholds or DEFAULT_THRESHOLDS
    configurations = [("lenient", 0.8), ("baseline", 1.0), ("strict", 1.2)]
    d = prepare_measurements(df)
    keys = [c for c in ["station_id", "station_name", "city", "state", "parameter"] if c in d.columns]
    # Build all threshold counts in one grouped Spark plan.
    elevated_counts = [
        F.sum(
            F.when(
                F.col("_value") >= _threshold_column({p: v * multiplier for p, v in base.items()})[F.col("parameter")],
                1,
            ).otherwise(0)
        ).alias(f"elevated_{name}")
        for name, multiplier in configurations
    ]
    aggregates = d.groupBy(*keys).agg(
        F.count("_value").alias("valid_observations"),
        F.round(F.mean("_value"), 4).alias("mean_concentration"),
        F.round(F.max("_value"), 4).alias("max_concentration"),
        *elevated_counts,
    )
    tables = []
    for name, multiplier in configurations:
        threshold_map = _threshold_column({p: v * multiplier for p, v in base.items()})
        tables.append(
            aggregates.withColumn("_threshold", threshold_map[F.col("parameter")])
            .withColumn("elevated_observations", F.col(f"elevated_{name}"))
            .withColumn("persistence_score", F.round(F.col("elevated_observations") / F.col("valid_observations") * 100, 2))
            .withColumn("coverage_sufficient", F.col("valid_observations") >= min_observations)
            .withColumn(
                "classification",
                F.when(~F.col("coverage_sufficient"), "insufficient_observations")
                .when(
                    (F.col("persistence_score") >= 50)
                    & (F.col("mean_concentration") >= F.col("_threshold"))
                    & (F.col("max_concentration") >= F.col("_threshold") * 1.5),
                    "persistent_hotspot",
                )
                .when(F.col("persistence_score") >= 25, "frequently_elevated")
                .when(F.col("max_concentration") >= F.col("_threshold"), "isolated_spike")
                .otherwise("ordinary_fluctuation"),
            )
            .withColumn("threshold_configuration", F.lit(name))
        )
    detail = tables[0].unionByName(tables[1]).unionByName(tables[2])
    counts = detail.groupBy("threshold_configuration", "classification").agg(
        F.count("*").alias("classification_count"),
        F.countDistinct("station_id").alias("classification_station_count"),
    )
    return (
        detail.join(counts, ["threshold_configuration", "classification"], "left")
        .select(
            *keys,
            "threshold_configuration",
            "classification",
            "persistence_score",
            "valid_observations",
            "elevated_observations",
            "classification_count",
            "classification_station_count",
        )
    )


def pollution_episodes(
    df: DataFrame,
    thresholds: Mapping[str, float] | None = None,
    max_gap_hours: int = 2,
    min_observations: int = 2,
) -> DataFrame:
    """Find consecutive elevated observations, breaking runs across large gaps."""
    thresholds = thresholds or DEFAULT_THRESHOLDS
    d = prepare_measurements(df).withColumn("_threshold", _threshold_column(thresholds)[F.col("parameter")])
    key = ["station_id", "parameter"]
    w = Window.partitionBy(*key).orderBy("_timestamp")
    d = d.withColumn("_previous_timestamp", F.lag("_timestamp").over(w)).withColumn(
        "_previous_elevated", F.lag(F.col("_value") >= F.col("_threshold")).over(w)
    ).withColumn(
        "_break",
        F.when(
            F.col("_previous_timestamp").isNull()
            | ~F.coalesce(F.col("_previous_elevated"), F.lit(False))
            | ((F.col("_timestamp").cast("long") - F.col("_previous_timestamp").cast("long")) > max_gap_hours * 3600),
            1,
        ).otherwise(0),
    ).withColumn("_episode_id", F.sum("_break").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    elevated = d.filter(F.col("_value") >= F.col("_threshold"))
    return elevated.groupBy(*(key + ["_episode_id"])).agg(
        F.min("_timestamp").alias("start_time"),
        F.max("_timestamp").alias("end_time"),
        F.round((F.unix_timestamp(F.max("_timestamp")) - F.unix_timestamp(F.min("_timestamp"))) / 3600.0, 2).alias("duration_hours"),
        F.round(F.mean("_value"), 4).alias("average_concentration"),
        F.round(F.max("_value"), 4).alias("maximum_concentration"),
        F.count("*").alias("elevated_observations"),
    ).filter(F.col("elevated_observations") >= min_observations).drop("_episode_id")


def spatial_hotspots(
    df: DataFrame,
    hotspots: DataFrame | None = None,
    grid_size: float = 0.1,
) -> DataFrame:
    """Produce map-ready station summaries and optional non-administrative grid cells."""
    d = prepare_measurements(df).filter(F.col("coordinate_valid"))
    station = d.groupBy(
        *[c for c in ["station_id", "station_name", "city", "state", "parameter"] if c in d.columns],
        "latitude", "longitude",
    ).agg(*_stats([]))
    if hotspots is not None:
        station = station.join(
            hotspots.select("station_id", "parameter", "classification", "persistence_score", "coverage_sufficient"),
            ["station_id", "parameter"],
            "left",
        )
    return station.withColumn("grid_latitude", F.floor(F.col("latitude") / grid_size) * grid_size).withColumn(
        "grid_longitude", F.floor(F.col("longitude") / grid_size) * grid_size
    )


def run_analytics(
    spark: SparkSession,
    input_path: str | Path = config.CLEAN_MEASUREMENTS,
    output_path: str | Path = config.PROCESSED_DIR / "analytics",
) -> dict[str, DataFrame]:
    """Read cleaned Parquet once, write all Member 2 outputs, and return result DataFrames."""
    input_root = Path(input_path)
    if input_root.is_dir():
        parquet_files = sorted(str(path) for path in input_root.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No Parquet files found below input path: {input_root}")
        measurements = spark.read.parquet(*parquet_files)
    else:
        measurements = spark.read.parquet(str(input_root))
    measurements = measurements.persist(StorageLevel.MEMORY_AND_DISK)
    outputs = {
        "temporal": temporal_analytics(measurements),
        "station_summary": station_summary(measurements),
    }
    outputs["station_rankings"] = station_rankings(outputs["station_summary"])
    outputs["hotspots"] = hotspot_classifications(measurements)
    outputs["episodes"] = pollution_episodes(measurements)
    outputs["spatial_hotspots"] = spatial_hotspots(measurements, outputs["hotspots"])
    outputs["threshold_sensitivity"] = threshold_sensitivity(measurements)
    root = Path(output_path)
    for name, result in outputs.items():
        result.write.mode("overwrite").parquet(str(root / name))
    measurements.unpersist()
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AirWatch Member 2 analytics.")
    parser.add_argument("--input", default=str(config.CLEAN_MEASUREMENTS))
    parser.add_argument("--output", default=str(config.PROCESSED_DIR / "analytics"))
    args = parser.parse_args()
    spark = get_spark("AirWatch-member2")
    run_analytics(spark, args.input, args.output)
    print(f"Analytics written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
