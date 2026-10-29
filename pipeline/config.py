"""Shared paths and settings for the AirWatch data pipeline (Member 1)."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = ROOT / "data" / "raw" / "xkdr"
PROCESSED_DIR = ROOT / "data" / "processed"
CLEAN_MEASUREMENTS = PROCESSED_DIR / "clean_measurements"
CLEAN_STATIONS = PROCESSED_DIR / "stations"
REPORTS_DIR = ROOT / "reports"

BASE_URL = "https://airquality.xkdr.org"
# Public demo key published in xKDR's README; only reads the 2024 bulk files.
DEMO_KEY = "aqi_demo_wbf92Qx21zX-Wa_Tg8Dx1nXe"

# Pollutants kept for analysis. Others (BTX etc.) are sparse and dropped.
CORE_PARAMETERS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone", "NH3"]

# Conservative sanity bounds: anything above is treated as a sensor error.
# Units follow xKDR's harmonised units (CO in mg/m3, the rest in ug/m3).
MAX_VALID = {
    "PM2.5": 1000.0,
    "PM10": 1000.0,
    "NO2": 1000.0,
    "SO2": 1000.0,
    "NH3": 1000.0,
    "Ozone": 1000.0,
    "CO": 100.0,
}

# A run of this many identical consecutive hourly readings is a stuck sensor.
STUCK_RUN_HOURS = 24

# Stations must fall inside India's bounding box to be mapped.
INDIA_LAT = (6.0, 38.0)
INDIA_LON = (68.0, 98.0)


def api_key() -> str:
    return os.environ.get("AQI_API_KEY") or DEMO_KEY
