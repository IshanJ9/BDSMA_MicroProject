"""Download the xKDR India Air Quality bulk Parquet files into data/raw/xkdr.

    python -m pipeline.ingest --start 2024-01 --end 2024-12

Set AQI_API_KEY (see .env.example) to read years other than 2024.
"""
import argparse
import sys

import requests

from pipeline import config

TIMEOUT = 120


def _headers() -> dict:
    return {"Authorization": f"Bearer {config.api_key()}"}


def list_files() -> list[dict]:
    r = requests.get(f"{config.BASE_URL}/v1/files", headers=_headers(), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()["files"]


def select_files(files: list[dict], start: str, end: str) -> list[dict]:
    """Stations/parameters plus measurement months with start <= YYYY-MM <= end."""
    chosen = [f for f in files if f["kind"] in ("stations", "parameters")]
    for f in files:
        if f["kind"] == "measurements" and start <= f"{f['year']}-{f['month']:02d}" <= end:
            chosen.append(f)
    return chosen


def download(key: str) -> str:
    dest = config.RAW_DIR / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(f"{config.BASE_URL}/v1/files/{key}", headers=_headers(), stream=True, timeout=TIMEOUT)
    if r.status_code == 403:
        sys.exit(f"403 for {key}: the demo key only reads 2024. Set AQI_API_KEY (see .env.example).")
    r.raise_for_status()
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
    tmp.replace(dest)
    return str(dest)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2024-01", help="first month, YYYY-MM")
    p.add_argument("--end", default="2024-12", help="last month, YYYY-MM")
    p.add_argument("--force", action="store_true", help="re-download existing files")
    args = p.parse_args()

    files = select_files(list_files(), args.start, args.end)
    n_meas = sum(f["kind"] == "measurements" for f in files)
    if n_meas == 0:
        sys.exit(f"No measurement files between {args.start} and {args.end}.")
    total = sum(f["bytes"] for f in files) / 1e6
    print(f"{len(files)} files ({n_meas} months), about {total:.1f} MB")
    for f in files:
        if (config.RAW_DIR / f["key"]).exists() and not args.force:
            print(f"skip   {f['key']}")
            continue
        print(f"get    {f['key']}")
        download(f["key"])


if __name__ == "__main__":
    main()
