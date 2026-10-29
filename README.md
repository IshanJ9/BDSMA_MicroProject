# AirWatch: Urban Pollution Hotspot Detection and Spatiotemporal Analytics

Big Data microproject. PySpark pipeline over India's CPCB air quality network, with a Streamlit dashboard.

| Module | Owner | Folder |
|---|---|---|
| 1. Dataset engineering and data quality | Member 1 | `pipeline/` |
| 2. Spatiotemporal analytics and hotspot detection | Member 2 | `analytics/` (to add) |
| 3. Dashboard and visualization | Member 3 | `dashboard/` (to add) |

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

## Outputs

`data/processed/clean_measurements/`, `data/processed/stations/` and `reports/data_quality_report.md`.
`data/` is gitignored (re-create it with the commands above); `reports/` is committed.
