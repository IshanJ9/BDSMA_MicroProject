from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.analytics_runner import (
    hotspot_classifications,
    pollution_episodes,
    spatial_hotspots,
    station_rankings,
    station_summary,
    threshold_sensitivity,
    temporal_analytics,
)
from pipeline.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    session = get_spark("AirWatch-member2-tests")
    yield session
    session.stop()


def measurements(spark, rows):
    schema = StructType(
        [
            StructField("station_id", StringType()),
            StructField("station_name", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("parameter", StringType()),
            StructField("unit", StringType()),
            StructField("value", DoubleType()),
            StructField("timestamp", TimestampType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def row(station, value, hour, lat=19.07, lon=72.87):
    return (station, "Station", "Mumbai", "Maharashtra", "PM2.5", "ug/m3", float(value),
            datetime(2024, 1, 1) + timedelta(hours=hour), float(lat), float(lon))


def test_temporal_and_station_statistics(spark):
    df = measurements(spark, [row("s1", 10, 0), row("s1", 30, 1), row("s1", 50, 24)])
    temporal = temporal_analytics(df)
    assert temporal.filter("temporal_granularity = 'hourly' AND period = '1'").count() == 1
    assert "is_unusually_elevated" in temporal.columns
    summary = station_summary(df)
    got = summary.first()
    assert got.observation_count == 3
    assert got.max_concentration == 50
    assert station_rankings(summary).first().mean_rank == 1


def test_persistence_hotspot_and_insufficient_coverage(spark):
    persistent = [row("persistent", 80, h) for h in range(24)]
    persistent += [row("persistent", 120, h + 24) for h in range(24)]
    insufficient = [row("short", 100, h) for h in range(3)]
    result = hotspot_classifications(measurements(spark, persistent + insufficient), min_observations=24)
    got = {r.station_id: r.classification for r in result.collect()}
    assert got["persistent"] == "persistent_hotspot"
    assert got["short"] == "insufficient_observations"


def test_episode_detection_breaks_on_missing_time(spark):
    rows = [row("s1", 80, 0), row("s1", 90, 1), row("s1", 100, 5), row("s1", 110, 6)]
    rows.append(("s1", "Station", "Mumbai", "Maharashtra", "PM2.5", "ug/m3", 100.0, None, 19.07, 72.87))
    episodes = pollution_episodes(measurements(spark, rows), max_gap_hours=2, min_observations=2)
    got = episodes.orderBy("start_time").collect()
    assert len(got) == 2
    assert got[0].elevated_observations == 2
    assert got[1].duration_hours == 1.0


def test_invalid_coordinates_are_excluded_from_spatial_output(spark):
    df = measurements(spark, [row("valid", 10, 0), row("invalid", 10, 0, 0, 0)])
    spatial = spatial_hotspots(df)
    assert spatial.select("station_id").distinct().collect()[0].station_id == "valid"


def test_sensitivity_and_empty_input(spark):
    df = measurements(spark, [row("s1", 80, h) for h in range(24)])
    sensitivity = threshold_sensitivity(df, min_observations=1)
    assert sensitivity.select("threshold_configuration").distinct().count() == 3
    assert "station_id" in sensitivity.columns
    empty = measurements(spark, [])
    assert temporal_analytics(empty).count() == 0
    assert hotspot_classifications(empty).count() == 0
