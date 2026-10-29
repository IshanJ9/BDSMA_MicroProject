"""Clean raw xKDR measurements with PySpark and write analysis-ready Parquet.

    python -m pipeline.clean

Reads  data/raw/xkdr/        (from pipeline.ingest)
Writes data/processed/clean_measurements/  partitioned by year, month
       data/processed/stations/
       reports/data_quality_report.{json,md}

Output schema of clean_measurements (the contract for Members 2 and 3):
    station_id, station_name, city, state, latitude, longitude, has_coordinates, coord_source,
    parameter, unit, value, timestamp (naive IST; CPCB on the hour, US embassy at :30),
    date, year, month, day, hour,
    day_of_week (1=Mon..7=Sun), source
"""
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipeline import config

OUTPUT_COLUMNS = [
    "station_id", "station_name", "city", "state", "latitude", "longitude", "has_coordinates",
    "coord_source", "parameter", "unit", "value", "timestamp", "date", "year", "month", "day", "hour",
    "day_of_week", "source",
]


def attach_stations(meas: DataFrame, stations: DataFrame, fallback: DataFrame | None = None) -> DataFrame:
    """Join station coordinates; prefer registry city/state, fall back to the row's own.

    Registry (CPCB) coordinates always win. `fallback` (station_id, latitude, longitude, precision) only
    fills stations the registry has no valid coordinates for, and is labelled in `coord_source`.
    """
    st = stations.select(
        "station_id",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("city_name").alias("reg_city"),
        F.col("state_name").alias("reg_state"),
    )
    j = meas.join(st, "station_id", "left")

    def in_india(lat, lon):
        return F.coalesce(F.col(lat).between(*config.INDIA_LAT) & F.col(lon).between(*config.INDIA_LON), F.lit(False))

    reg_ok = in_india("latitude", "longitude")
    if fallback is not None:
        fb = fallback.select(
            "station_id",
            F.col("latitude").cast("double").alias("fb_lat"),
            F.col("longitude").cast("double").alias("fb_lon"),
            F.concat(F.lit("geocoded_"), F.col("precision")).alias("fb_source"),
        )
        j = j.join(fb, "station_id", "left")
        fb_ok = in_india("fb_lat", "fb_lon")
    else:
        j = j.withColumn("fb_lat", F.lit(None).cast("double")).withColumn("fb_lon", F.lit(None).cast("double"))
        j = j.withColumn("fb_source", F.lit(None).cast("string"))
        fb_ok = F.lit(False)
    use_fb = ~reg_ok & fb_ok
    return (
        j.withColumn("city", F.coalesce("reg_city", "city_name"))
        .withColumn("state", F.coalesce("reg_state", "state_name"))
        .withColumn("coord_source", F.when(reg_ok, F.lit("cpcb_registry")).when(use_fb, F.col("fb_source")))
        .withColumn("latitude", F.when(reg_ok, F.col("latitude")).when(use_fb, F.col("fb_lat")))
        .withColumn("longitude", F.when(reg_ok, F.col("longitude")).when(use_fb, F.col("fb_lon")))
        .withColumn("has_coordinates", F.col("coord_source").isNotNull())
        .drop("reg_city", "reg_state", "city_name", "state_name", "fb_lat", "fb_lon", "fb_source")
    )


def flag_stuck(df: DataFrame, run_hours: int = config.STUCK_RUN_HOURS) -> DataFrame:
    """Mark readings inside a run of >= run_hours identical values (the first of the run is kept)."""
    w = Window.partitionBy("station_id", "parameter").orderBy("timestamp")
    prev = F.lag("value").over(w)
    changed = (prev.isNull() | (F.col("value") != prev)).cast("int")
    d = df.withColumn("_run", F.sum(changed).over(w))
    wr = Window.partitionBy("station_id", "parameter", "_run")
    d = d.withColumn("_run_len", F.count(F.lit(1)).over(wr)).withColumn(
        "_pos", F.row_number().over(wr.orderBy("timestamp"))
    )
    stuck = (F.col("_run_len") >= run_hours) & (F.col("_pos") > 1)
    return d.withColumn("is_stuck", stuck).drop("_run", "_run_len", "_pos")


def clean_measurements(
    raw: DataFrame, stations: DataFrame, counts: dict | None = None, fallback: DataFrame | None = None
) -> DataFrame:
    """Apply all cleaning rules. If `counts` is given it is filled with row counts per stage."""
    counts = counts if counts is not None else {}

    df = raw.select(
        "station_id",
        "station_name",
        "city_name",
        "state_name",
        F.col("parameter_name").alias("parameter"),
        "unit",
        # Keep as naive (wall-clock IST) so Parquet readers see the same time, not a UTC shift.
        F.col("collected_at").cast("timestamp_ntz").alias("timestamp"),
        F.col("value").cast("double").alias("value"),
        "source",
    )
    counts["raw_rows"] = df.count()

    df = df.filter(F.col("parameter").isin(config.CORE_PARAMETERS))
    counts["after_parameter_filter"] = df.count()

    df = df.dropna(subset=["station_id", "parameter", "timestamp", "value"]).filter(~F.isnan("value"))
    counts["after_null_drop"] = df.count()

    df = df.dropDuplicates(["station_id", "parameter", "timestamp"])
    counts["after_dedup"] = df.count()

    max_valid = F.create_map(*[x for k, v in config.MAX_VALID.items() for x in (F.lit(k), F.lit(v))])
    df = df.filter((F.col("value") >= 0) & (F.col("value") <= max_valid[F.col("parameter")]))
    counts["after_range_filter"] = df.count()

    df = flag_stuck(df).filter(~F.col("is_stuck")).drop("is_stuck")
    counts["after_stuck_filter"] = df.count()

    df = attach_stations(df, stations, fallback)
    ts = F.col("timestamp")
    df = (
        df.withColumn("date", F.to_date(ts))
        .withColumn("year", F.year(ts))
        .withColumn("month", F.month(ts))
        .withColumn("day", F.dayofmonth(ts))
        .withColumn("hour", F.hour(ts))
        .withColumn("day_of_week", ((F.dayofweek(ts) + 5) % 7) + 1)
    )
    return df.select(*OUTPUT_COLUMNS)


def main() -> None:
    from pipeline.quality import build_report, write_report
    from pipeline.spark_session import get_spark

    spark = get_spark("AirWatch-clean")
    meas_files = sorted(config.RAW_DIR.glob("v1/measurements/year=*/month=*/data.parquet"))
    if not meas_files:
        raise SystemExit("No raw files found. Run: python -m pipeline.ingest")

    raw = spark.read.parquet(*[p.as_posix() for p in meas_files])
    stations = spark.read.parquet((config.RAW_DIR / "v1" / "stations.parquet").as_posix())

    fallback = None
    if config.GEOCODED_STATIONS.exists():
        fallback = spark.read.csv(config.GEOCODED_STATIONS.as_posix(), header=True)

    counts: dict = {}
    t0 = time.perf_counter()
    clean = clean_measurements(raw, stations, counts, fallback)
    clean.write.mode("overwrite").partitionBy("year", "month").parquet(config.CLEAN_MEASUREMENTS.as_posix())

    clean = spark.read.parquet(config.CLEAN_MEASUREMENTS.as_posix())
    counts["final_rows"] = clean.count()

    st_out = clean.groupBy(
        "station_id", "station_name", "city", "state", "latitude", "longitude", "has_coordinates", "coord_source"
    ).agg(
        F.count("*").alias("n_readings"),
        F.min("timestamp").alias("first_reading"),
        F.max("timestamp").alias("last_reading"),
        F.collect_set("parameter").alias("parameters"),
    )
    st_out.write.mode("overwrite").parquet(config.CLEAN_STATIONS.as_posix())
    t1 = time.perf_counter()

    perf = {
        "input_files": len(meas_files),
        "input_mb": sum(p.stat().st_size for p in meas_files) / 1e6,
        "clean_write_seconds": t1 - t0,
        "cores": spark.sparkContext.defaultParallelism,
        "driver_memory": spark.conf.get("spark.driver.memory"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
    }
    report, completeness_rows = build_report(raw, clean, counts, perf)
    perf["report_seconds"] = time.perf_counter() - t1
    write_report(report, completeness_rows, config.REPORTS_DIR)
    print(f"Done. {counts['raw_rows']:,} raw -> {counts['final_rows']:,} clean rows.")
    print(f"Report: {config.REPORTS_DIR / 'data_quality_report.md'}")
    spark.stop()


if __name__ == "__main__":
    main()
