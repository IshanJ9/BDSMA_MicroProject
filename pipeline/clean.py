"""Clean raw xKDR measurements with PySpark and write analysis-ready Parquet.

    python -m pipeline.clean

Reads  data/raw/xkdr/        (from pipeline.ingest)
Writes data/processed/clean_measurements/  partitioned by year, month
       data/processed/stations/
       reports/data_quality_report.{json,md}

Output schema of clean_measurements (the contract for Members 2 and 3):
    station_id, station_name, city, state, latitude, longitude, has_coordinates,
    parameter, unit, value, timestamp (naive IST; CPCB on the hour, US embassy at :30),
    date, year, month, day, hour,
    day_of_week (1=Mon..7=Sun), source
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipeline import config

OUTPUT_COLUMNS = [
    "station_id", "station_name", "city", "state", "latitude", "longitude", "has_coordinates",
    "parameter", "unit", "value", "timestamp", "date", "year", "month", "day", "hour",
    "day_of_week", "source",
]


def attach_stations(meas: DataFrame, stations: DataFrame) -> DataFrame:
    """Join registry coordinates; prefer registry city/state, fall back to the row's own."""
    st = stations.select(
        "station_id",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("city_name").alias("reg_city"),
        F.col("state_name").alias("reg_state"),
    )
    j = meas.join(st, "station_id", "left")
    in_india = F.col("latitude").between(*config.INDIA_LAT) & F.col("longitude").between(*config.INDIA_LON)
    return (
        j.withColumn("city", F.coalesce("reg_city", "city_name"))
        .withColumn("state", F.coalesce("reg_state", "state_name"))
        .withColumn("has_coordinates", F.coalesce(in_india, F.lit(False)))
        .withColumn("latitude", F.when(F.col("has_coordinates"), F.col("latitude")))
        .withColumn("longitude", F.when(F.col("has_coordinates"), F.col("longitude")))
        .drop("reg_city", "reg_state", "city_name", "state_name")
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


def clean_measurements(raw: DataFrame, stations: DataFrame, counts: dict | None = None) -> DataFrame:
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

    df = attach_stations(df, stations)
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

    counts: dict = {}
    clean = clean_measurements(raw, stations, counts)
    clean.write.mode("overwrite").partitionBy("year", "month").parquet(config.CLEAN_MEASUREMENTS.as_posix())

    clean = spark.read.parquet(config.CLEAN_MEASUREMENTS.as_posix())
    counts["final_rows"] = clean.count()

    st_out = clean.groupBy(
        "station_id", "station_name", "city", "state", "latitude", "longitude", "has_coordinates"
    ).agg(
        F.count("*").alias("n_readings"),
        F.min("timestamp").alias("first_reading"),
        F.max("timestamp").alias("last_reading"),
        F.collect_set("parameter").alias("parameters"),
    )
    st_out.write.mode("overwrite").parquet(config.CLEAN_STATIONS.as_posix())

    report = build_report(raw, clean, counts)
    write_report(report, config.REPORTS_DIR)
    print(f"Done. {counts['raw_rows']:,} raw -> {counts['final_rows']:,} clean rows.")
    print(f"Report: {config.REPORTS_DIR / 'data_quality_report.md'}")
    spark.stop()


if __name__ == "__main__":
    main()
