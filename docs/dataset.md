# Dataset: India Air Quality Database (xKDR Forum)

| | |
|---|---|
| Publisher | XKDR Forum, compiled from CPCB CAAQM and US Department of State (AirNow) |
| Home / repo | https://airquality.xkdr.org · https://github.com/xKDR/Air-Quality-Database |
| Licence | CC BY 4.0. Cite as *India Air Quality Database, XKDR Forum*, sourced from the Central Pollution Control Board (CAAQM network) and the US Department of State via AirNow |
| Coverage | 553 CPCB stations + 5 US Embassy monitors; 2009 to 2025 (dense through 2024); about 196 M hourly rows |
| Format | Monthly Parquet files (`year=YYYY/month=MM/data.parquet`), plus `stations.parquet` and `parameters.parquet` |
| Access | Free API key at https://airquality.xkdr.org/signup. The public demo key reads 2024 files only |

## Raw schema (measurements)

`station_id, station_name, state_name, city_name, parameter_name, unit, collected_at, value, source`

- `collected_at` is a **naive timestamp in Indian Standard Time**. CPCB readings sit on the hour.
- `value` is as reported by the network: no gap filling, outlier removal or calibration.
- `stations.parquet` adds `latitude`, `longitude`, `first_seen`, `last_seen`, `n_rows`, `parameters`.
  62 decommissioned stations have null coordinates.

## Why this dataset

It has all three things the spatiotemporal analysis needs (timestamp, station coordinates, pollutant value) in one
consistent schema, and it is large enough to justify PySpark. The Mumbai hourly files from
data.opencity.in were considered too: they have no coordinates, and the 2017–2023 files are in a wide
month-by-hour spreadsheet layout. data.gov.in's real-time AQI resource is a live snapshot, not a history.

## Cleaning rules (see `pipeline/clean.py`, thresholds in `pipeline/config.py`)

1. Keep core pollutants: PM2.5, PM10, NO2, SO2, CO, Ozone, NH3.
2. Drop rows with a null/NaN station, parameter, timestamp or value.
3. Drop duplicates on (station, parameter, timestamp).
4. Drop negative values and values above a conservative per-pollutant ceiling (1000 µg/m³; CO 100 mg/m³).
5. Drop stuck-sensor runs: 24 or more identical consecutive readings keep only the first.
6. Join station coordinates. Coordinates outside India's bounding box (lat 6–38, lon 68–98) are nulled and the
   row is flagged `has_coordinates = false`. Those rows stay in the data for time-based charts but cannot be mapped.

Rules 4 and 5 are our own sanity bounds, in the spirit of the QA/QC in Sharma & Mauzerall (2022),
*Aerosol and Air Quality Research*; they are not official CPCB validation.

## Output contract for Members 2 and 3

`data/processed/clean_measurements/` (Parquet, partitioned by `year`, `month`):

| Column | Type | Note |
|---|---|---|
| station_id, station_name, city, state | string | |
| latitude, longitude | double | null when `has_coordinates` is false |
| has_coordinates | boolean | |
| parameter | string | PM2.5, PM10, NO2, SO2, CO, Ozone, NH3 |
| unit | string | µg/m³, except CO in mg/m³ |
| value | double | |
| timestamp | timestamp | hourly, IST |
| date, year, month, day, hour | | derived from `timestamp` |
| day_of_week | int | 1 = Monday … 7 = Sunday |
| source | string | cpcb_caaqm or us_embassy |

`data/processed/stations/` has one row per station with coordinates, reading count, first/last reading and the
parameters it reports. `reports/data_quality_report.md` shows what each cleaning step removed.
