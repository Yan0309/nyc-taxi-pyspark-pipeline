"""Data source helpers."""

from __future__ import annotations

import glob
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def input_files(pattern: str) -> list[str]:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No Parquet files matched {pattern!r}. "
            "Run `python scripts/download_data.py` first."
        )
    return files


def compressed_size_bytes(pattern: str) -> int:
    return sum(Path(path).stat().st_size for path in input_files(pattern))


def read_trips(spark: SparkSession, pattern: str) -> DataFrame:
    """Read all source files while leaving schema merge disabled for speed."""
    input_files(pattern)
    return spark.read.parquet(pattern)


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    """Read and normalize the small TLC taxi-zone lookup."""
    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
    )
    return raw.select(
        F.col("LocationID").cast("int").alias("pickup_location_id"),
        F.trim(F.col("Borough")).alias("pickup_borough"),
        F.trim(F.col("Zone")).alias("pickup_zone"),
        F.trim(F.col("service_zone")).alias("pickup_service_zone"),
    ).filter(F.col("pickup_location_id").isNotNull())
