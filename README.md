# AirWatch: Urban Pollution Hotspot Detection and Spatiotemporal Analytics

Big Data microproject. PySpark pipeline over India's CPCB air quality network, with a Streamlit dashboard.

| Module | Owner | Folder |
|---|---|---|
| 1. Dataset engineering and data quality | Member 1 | `pipeline/` |
| 2. Spatiotemporal analytics and hotspot detection | Member 2 | `analytics/` |
| 3. Dashboard and visualization | Member 3 | `dashboard/` |

Dataset details, cleaning rules and the output schema other modules read: [docs/dataset.md](docs/dataset.md).

## Run with Docker (recommended: same on Windows, macOS and Linux)

Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it. Nothing else is needed
(Java and PySpark are inside the image).

```bash
docker compose build
docker compose run --rm pipeline python -m pipeline.ingest --start 2024-01 --end 2024-12
docker compose run --rm pipeline python -m pipeline.clean
docker compose run --rm pipeline python -m pytest
```

The project folder is mounted into the container, so `data/` and `reports/` appear on your machine.

A full year (about 47M raw rows) took about 5 minutes and needs roughly 6 GB of RAM available to Docker
(Docker Desktop > Settings > Resources). Spark's driver memory defaults to 4 GB; override it with the
`AIRWATCH_DRIVER_MEMORY` environment variable.

Optional: get a free key at https://airquality.xkdr.org/signup and put `AQI_API_KEY=...` in a `.env` file
(see `.env.example`) to read years other than 2024. The built-in demo key only reads 2024.

## Run without Docker (Linux / macOS)

Needs Python 3.10+ and Java 17 or 21.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pipeline.ingest --start 2024-01 --end 2024-12
python -m pipeline.clean
python -m pytest

```

On Windows, Spark cannot write Parquet without Hadoop's `winutils.exe`; use Docker (or WSL) instead.

## Member 2: spatiotemporal analytics and hotspot detection

Run the analytics after the cleaned Parquet dataset has been created:

```bash
docker compose run --rm pipeline python -m analytics.analytics_runner
```

The implementation reads `data/processed/clean_measurements/` directly and writes
Parquet outputs below `data/processed/analytics/`:

- `temporal/`: hourly, daily, monthly, and weekday/weekend concentration statistics.
- `station_summary/` and `station_rankings/`: pollutant-specific magnitude statistics and
  observation-count-aware rankings.
- `hotspots/`: persistence-aware station classifications.
- `episodes/`: consecutive elevated measurement periods.
- `spatial_hotspots/`: coordinate-validated, map-compatible station summaries with
  0.1-degree analytical grid labels (not administrative boundaries).
- `threshold_sensitivity/`: station-level classifications under lenient (0.8x),
  baseline (1.0x), and strict (1.2x) thresholds, including classification counts.

Hotspot thresholds are pollutant-specific concentration thresholds, not AQI values.
The persistence score is `100 * elevated observations / valid observations`. A station
is a `persistent_hotspot` only when it meets the minimum observation count, has at
least 50% elevated observations, has a mean at or above its pollutant threshold, and
reaches 1.5 times that threshold at least once. Lower-frequency exceedances are
reported as `isolated_spike` or `frequently_elevated`; insufficient series are never
classified as hotspots. Episode detection breaks runs when observations are more than
two hours apart, so missing readings are not silently treated as continuous pollution.
Coordinates outside India's configured bounds are excluded from spatial outputs.
Threshold sensitivity indicates whether classifications depend strongly on the
selected concentration threshold. These thresholds describe measured concentrations
and should not be interpreted as regulatory limits or AQI.

On Windows, run through Docker or WSL as described above. The local Spark
validation environment requires Hadoop `winutils.exe` for direct Parquet
directory access; no full-dataset output is claimed unless the Docker/WSL
command completes successfully.

## Member 3: Dashboard and Visualization Layer

The dashboard is an environmental intelligence platform built with Streamlit, Plotly, and Folium.
It consumes the analytical Parquet outputs produced by Member 2 without loading raw big datasets into memory.

### Run the Dashboard locally
```bash
streamlit run dashboard/app.py
```
Open your browser at `http://localhost:8501`.

### Run the Dashboard with Docker
```bash
docker compose up dashboard
```
Open your browser at `http://localhost:8501`.

### Dashboard Architecture
- `dashboard/app.py`: Streamlit multi-section application, sidebar filters, environmental UI theme, and KPI cards.
- `dashboard/data_loader.py`: Cached Parquet loaders for Member 2 analytical outputs with error handling and fallback support.
- `dashboard/charts.py`: Interactive Plotly charts (diurnal cycles, time-series, monthly trends, weekday vs weekend, day-hour heatmaps, episode durations, rankings, and sensitivity).
- `dashboard/map_visualization.py`: Folium interactive spatiotemporal hotspot map with station popups, coordinate validation notes, and dynamic legend.
- `dashboard/utils.py`: Unit conversions, thresholds, color schemes, data filtering, and formatting utilities.
- `dashboard/README.md`: Comprehensive user manual, architecture details, and academic presentation/viva guide.

## Outputs

- Member 1: `data/processed/clean_measurements/`, `data/processed/stations/`, `reports/data_quality_report.md`
- Member 2: `data/processed/analytics/` (`temporal/`, `station_summary/`, `station_rankings/`, `hotspots/`, `episodes/`, `spatial_hotspots/`, `threshold_sensitivity/`)
- Member 3: Interactive Streamlit analytics dashboard at `dashboard/app.py`
