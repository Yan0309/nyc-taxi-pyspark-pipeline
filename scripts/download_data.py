#!/usr/bin/env python3
"""Download NYC TLC yellow-taxi and taxi-zone source data."""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_URL = BASE_URL + "/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = BASE_URL + "/misc/taxi_zone_lookup.csv"


def parse_month(value: str) -> tuple[int, int]:
    try:
        year_text, month_text = value.split("-", maxsplit=1)
        year, month = int(year_text), int(month_text)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM, received {value!r}"
        ) from exc
    if not 2009 <= year <= 2100 or not 1 <= month <= 12:
        raise argparse.ArgumentTypeError(f"Month out of range: {value!r}")
    return year, month


def iter_months(start: tuple[int, int], end: tuple[int, int]):
    year, month = start
    while (year, month) <= end:
        yield f"{year:04d}-{month:02d}"
        month += 1
        if month == 13:
            year, month = year + 1, 1


def download(url: str, destination: Path, retries: int = 3) -> tuple[Path, int]:
    if destination.exists() and destination.stat().st_size > 0:
        print(f"exists  {destination.name} ({destination.stat().st_size:,} bytes)")
        return destination, destination.stat().st_size

    temporary = destination.with_suffix(destination.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "codex-pyspark-pipeline/1.0"}
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                expected = int(response.headers.get("Content-Length", "0"))
                with temporary.open("wb") as handle:
                    while chunk := response.read(1024 * 1024):
                        handle.write(chunk)
            actual = temporary.stat().st_size
            if expected and actual != expected:
                raise OSError(f"expected {expected:,} bytes, got {actual:,}")
            temporary.replace(destination)
            print(f"saved   {destination.name} ({actual:,} bytes)")
            return destination, actual
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            print(
                f"retry {attempt}/{retries} for {destination.name}: {exc}",
                file=sys.stderr,
            )
    raise RuntimeError(f"Could not download {url}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-month", type=parse_month, default=parse_month("2023-05")
    )
    parser.add_argument(
        "--end-month", type=parse_month, default=parse_month("2024-12")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/raw")
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    if args.start_month > args.end_month:
        parser.error("--start-month must not be after --end-month")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    months = list(iter_months(args.start_month, args.end_month))
    tasks = []
    for month in months:
        name = f"yellow_tripdata_{month}.parquet"
        tasks.append((TRIP_URL.format(month=month), args.output_dir / name))

    zone_path = args.output_dir / "taxi_zone_lookup.csv"
    tasks.append((ZONE_URL, zone_path))

    total_bytes = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, args.workers)
    ) as executor:
        futures = [
            executor.submit(download, url, destination)
            for url, destination in tasks
        ]
        for future in concurrent.futures.as_completed(futures):
            _, size = future.result()
            total_bytes += size

    print(
        f"\nDownloaded/verified {len(months)} trip files plus zones: "
        f"{total_bytes / (1024 ** 3):.3f} GiB on disk."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
