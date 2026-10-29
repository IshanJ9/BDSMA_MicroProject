from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from pipeline.clean import clean_measurements, flag_stuck
from pipeline.spark_session import get_spark

RAW_COLS = ["station_id", "station_name", "city_name", "state_name", "parameter_name", "unit",
            "collected_at", "value", "source"]
STATION_COLS = ["station_id", "latitude", "longitude", "city_name", "state_name"]
T0 = datetime(2024, 1, 1, 0, 0)  # a Monday


@pytest.fixture(scope="module")
def spark():
    s = get_spark("AirWatch-tests")
    yield s
    s.stop()


def raw_row(station="site_1", param="PM2.5", hour=0, value=50.0, unit="µg/m³"):
    ts = (T0 + timedelta(hours=hour)).strftime("%Y-%m-%d %H:%M:%S")  # string: no timezone involved
    return (station, "Stn", "Mumbai", "Maharashtra", param, unit, ts, value, "cpcb_caaqm")


def stations(spark):
    return spark.createDataFrame(
        [("site_1", 19.07, 72.87, "Mumbai", "Maharashtra"), ("site_2", None, None, None, None),
         ("site_3", 0.0, 0.0, "Nowhere", "Nowhere")],
        STATION_COLS,
    )


def run(spark, rows):
    counts = {}
    # The real raw files hold naive (timezone-less) timestamps, so build the test data the same way.
    raw = spark.createDataFrame(rows, RAW_COLS).withColumn("collected_at", F.col("collected_at").cast("timestamp_ntz"))
    out = clean_measurements(raw, stations(spark), counts)
    return out, counts


def test_drops_nulls_nan_duplicates_and_non_core(spark):
    rows = [
        raw_row(hour=0, value=40.0),
        raw_row(hour=0, value=40.0),               # duplicate
        raw_row(hour=1, value=None),               # null value
        raw_row(hour=2, value=float("nan")),       # NaN
        raw_row(hour=3, param="Benzene", value=1.0),  # not a core pollutant
        raw_row(hour=4, value=41.0),
    ]
    out, counts = run(spark, rows)
    assert sorted(r.hour for r in out.collect()) == [0, 4]
    assert counts["raw_rows"] == 6 and counts["after_dedup"] == 2


def test_range_filter(spark):
    rows = [
        raw_row(hour=0, value=-5.0),
        raw_row(hour=1, value=1500.0),
        raw_row(hour=2, value=999.0),
        raw_row(hour=3, param="CO", value=150.0),
        raw_row(hour=4, param="CO", value=1.2),
    ]
    out, _ = run(spark, rows)
    assert sorted((r.parameter, r.value) for r in out.collect()) == [("CO", 1.2), ("PM2.5", 999.0)]


def test_stuck_sensor_run_keeps_first_reading_only(spark):
    stuck = [raw_row(hour=h, value=77.0) for h in range(30)]       # 30 identical hours
    varying = [raw_row(station="site_2", hour=h, value=10.0 + h) for h in range(30)]
    out, _ = run(spark, stuck + varying)
    by_station = {r["station_id"]: r["n"] for r in out.groupBy("station_id").agg(F.count("*").alias("n")).collect()}
    assert by_station == {"site_1": 1, "site_2": 30}


def test_short_identical_run_is_kept(spark):
    df = spark.createDataFrame([raw_row(hour=h, value=5.0) for h in range(10)], RAW_COLS).select(
        "station_id", F.col("parameter_name").alias("parameter"),
        F.col("collected_at").alias("timestamp"), "value")
    assert flag_stuck(df).filter("is_stuck").count() == 0


def test_station_join_and_coordinate_validation(spark):
    rows = [raw_row(station="site_1"), raw_row(station="site_2"), raw_row(station="site_3"),
            raw_row(station="unknown")]
    out, _ = run(spark, rows)
    got = {r.station_id: r for r in out.collect()}
    assert got["site_1"].has_coordinates and got["site_1"].latitude == pytest.approx(19.07)
    for sid in ("site_2", "site_3", "unknown"):  # null, outside India, not in registry
        assert not got[sid].has_coordinates and got[sid].latitude is None
    assert got["site_2"].city == "Mumbai"  # falls back to the measurement row's city


def test_geocoded_fallback_fills_only_missing_coordinates(spark):
    fb = spark.createDataFrame(
        [("site_1", "10.0", "70.0", "city"),      # registry has valid coordinates: must not be overridden
         ("site_2", "18.5", "73.8", "locality"),  # registry null: fallback applies
         ("site_3", "0.0", "0.0", "city")],       # fallback outside India: rejected
        ["station_id", "latitude", "longitude", "precision"],
    )
    raw = spark.createDataFrame([raw_row(station=s) for s in ("site_1", "site_2", "site_3")], RAW_COLS).withColumn(
        "collected_at", F.col("collected_at").cast("timestamp_ntz"))
    got = {r.station_id: r for r in clean_measurements(raw, stations(spark), None, fb).collect()}
    assert (got["site_1"].latitude, got["site_1"].coord_source) == (pytest.approx(19.07), "cpcb_registry")
    assert (got["site_2"].latitude, got["site_2"].coord_source) == (pytest.approx(18.5), "geocoded_locality")
    assert got["site_2"].has_coordinates
    assert got["site_3"].coord_source is None and not got["site_3"].has_coordinates


def test_completeness_is_share_of_period_hours(spark):
    from pipeline.quality import completeness

    full = [raw_row(station="site_1", hour=h, value=10.0 + h) for h in range(48)]    # 2 days, every hour
    partial = [raw_row(station="site_2", hour=h, value=10.0 + h) for h in range(12)]  # 12 of 48 hours
    out, _ = run(spark, full + partial)
    got = {r.station_id: r for r in completeness(out).collect()}
    assert got["site_1"].period_hours == 48 and got["site_1"].completeness == 1.0
    assert got["site_2"].hours_with_data == 12 and got["site_2"].completeness == 0.25


def test_time_features(spark):
    out, _ = run(spark, [raw_row(hour=5), raw_row(hour=30, value=51.0)])  # Mon 05:00, Tue 06:00
    got = {r.timestamp: r for r in out.collect()}
    mon, tue = got[T0 + timedelta(hours=5)], got[T0 + timedelta(hours=30)]
    assert (mon.day_of_week, mon.hour, mon.year, mon.month) == (1, 5, 2024, 1)
    assert (tue.day_of_week, tue.hour, tue.day) == (2, 6, 2)
