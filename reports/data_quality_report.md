# Data quality report

Period: 2024-10-01 00:00:00 to 2024-12-31 23:30:00 (IST)

## Cleaning funnel

| Stage | Rows | % of raw |
|---|---:|---:|
| Raw rows (all parameters) | 12,332,239 | 100.0% |
| After keeping core pollutants | 7,096,328 | 57.5% |
| After dropping null/NaN fields | 7,096,328 | 57.5% |
| After removing duplicates | 7,096,328 | 57.5% |
| After removing negative / out-of-range values | 7,096,321 | 57.5% |
| After removing stuck-sensor runs | 7,055,141 | 57.2% |
| Final clean rows | 7,055,141 | 57.2% |

## Coverage

- Stations in clean data: 532
- Stations without valid coordinates (excluded from maps): 48
- Parameters with mixed units: none

## Per pollutant

| Parameter | Unit | Rows | Stations | Mean | Median | Max |
|---|---|---:|---:|---:|---:|---:|
| CO | mg/m³ | 1,023,983 | 516 | 0.94 | 0.72 | 29.28 |
| NH3 | µg/m³ | 941,440 | 473 | 23.26 | 16.8 | 494.66 |
| NO2 | µg/m³ | 1,044,361 | 526 | 26.14 | 17.88 | 497.6 |
| Ozone | µg/m³ | 992,141 | 502 | 30.74 | 20.83 | 490.6 |
| PM10 | µg/m³ | 1,014,039 | 517 | 135.8 | 108.75 | 1000.0 |
| PM2.5 | µg/m³ | 1,033,984 | 525 | 69.24 | 51.44 | 1000.0 |
| SO2 | µg/m³ | 1,005,193 | 507 | 12.45 | 8.8 | 199.8 |

## Raw null counts (before cleaning)

- `station_id`: 0
- `collected_at`: 0
- `parameter_name`: 0
- `value`: 0
