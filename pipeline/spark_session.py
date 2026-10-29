"""One place to build the SparkSession so every script behaves the same."""
import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "AirWatch") -> SparkSession:
    # Make workers use the same interpreter as the driver (matters on Windows/venv).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
