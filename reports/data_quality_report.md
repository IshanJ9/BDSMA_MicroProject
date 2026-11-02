# Data quality report

Period: 2024-01-01 00:00:00 to 2024-12-31 23:30:00 (IST)

## Cleaning funnel

| Stage | Rows | % of raw |
|---|---:|---:|
| Raw rows (all parameters) | 46,937,170 | 100.0% |
| After keeping core pollutants | 27,062,284 | 57.7% |
| After dropping null/NaN fields | 27,062,284 | 57.7% |
| After removing duplicates | 27,062,284 | 57.7% |
| After removing negative / out-of-range values | 27,062,260 | 57.7% |
| After removing stuck-sensor runs | 26,904,967 | 57.3% |
| Final clean rows | 26,904,967 | 57.3% |

## Coverage

- Stations in clean data: 538
- Stations without valid coordinates (excluded from maps): 1
- Stations by coordinate source: cpcb_registry 485, geocoded_city 27, geocoded_locality 25, none 1
- Parameters with mixed units: none

## Per pollutant

| Parameter | Unit | Rows | Stations | Mean | Median | Max |
|---|---|---:|---:|---:|---:|---:|
| CO | mg/m³ | 3,913,305 | 528 | 0.81 | 0.63 | 48.28 |
| NH3 | µg/m³ | 3,576,077 | 476 | 22.45 | 16.3 | 498.39 |
| NO2 | µg/m³ | 3,973,333 | 533 | 22.23 | 15.4 | 497.77 |
| Ozone | µg/m³ | 3,818,673 | 513 | 31.31 | 22.48 | 490.6 |
| PM10 | µg/m³ | 3,854,200 | 526 | 108.25 | 82.25 | 1000.0 |
| PM2.5 | µg/m³ | 3,919,988 | 534 | 50.4 | 35.48 | 1000.0 |
| SO2 | µg/m³ | 3,849,391 | 513 | 12.37 | 8.62 | 199.9 |

## Missing data (completeness)

Completeness = hours with a valid reading / 8,784 hours in the period. Missing readings are absent rows in the source rather than nulls, and readings removed by cleaning count as missing. A series is *usable* at 75% or more. Per-series values: `reports/station_completeness.csv`.

| Parameter | Station series | Usable | Median completeness | 90%+ | 75-90% | 50-75% | below 50% |
|---|---:|---:|---:|---:|---:|---:|---:|
| CO | 528 | 431 (82%) | 93% | 337 | 94 | 48 | 49 |
| NH3 | 476 | 397 (83%) | 93% | 291 | 106 | 46 | 33 |
| NO2 | 533 | 436 (82%) | 93% | 330 | 106 | 52 | 45 |
| Ozone | 513 | 426 (83%) | 93% | 318 | 108 | 44 | 43 |
| PM10 | 526 | 423 (80%) | 92% | 299 | 124 | 54 | 49 |
| PM2.5 | 534 | 431 (81%) | 92% | 297 | 134 | 54 | 49 |
| SO2 | 513 | 434 (85%) | 93% | 316 | 118 | 42 | 37 |

## Processing performance

- Input: 12 Parquet files, 88 MB on disk, 46,937,170 rows
- Cleaning, funnel counts and write: 102 s (460,343 raw rows/s)
- Quality report (aggregations over the clean data): 28 s
- Spark: local mode, 10 cores, driver memory 4g, 32 shuffle partitions
- The funnel counts run extra Spark jobs over the data, so a run without them would be faster.

## Raw null counts (before cleaning)

- `station_id`: 0
- `collected_at`: 0
- `parameter_name`: 0
- `value`: 0
- All zero: the source leaves gaps as absent rows, so see the completeness table above.
