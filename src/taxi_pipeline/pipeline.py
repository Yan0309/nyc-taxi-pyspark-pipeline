"""End-to-end PySpark pipeline with physical-plan and timing capture."""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import shutil
import statistics
import sys
import time
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.analytics import (
    CACHE_BENCHMARK_SQL,
    DAILY_BOROUGH_SQL,
    MONTHLY_HOURLY_DEMAND_SQL,
    TOP_EVENING_ZONES_SQL,
)
from taxi_pipeline.io import (
    compressed_size_bytes,
    input_files,
    read_trips,
    read_zones,
)
from taxi_pipeline.spark_session import build_spark_session
from taxi_pipeline.transform import (
    add_trip_measures,
    apply_early_filters,
    build_zone_hour_metrics,
    join_zone_lookup,
    select_source_columns,
)

LOGGER = logging.getLogger("taxi_pipeline")


@dataclass(frozen=True)
class PipelineConfig:
    input_glob: str = "data/raw/yellow_tripdata_*.parquet"
    zone_lookup: str = "data/raw/taxi_zone_lookup.csv"
    output_dir: Path = Path("artifacts/run")
    master: str = "local[*]"
    shuffle_partitions: int = 48
    sample_fraction: float = 1.0
    ui_pause_seconds: int = 0
    skip_cache_benchmark: bool = False
    benchmark_repeats: int = 5


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S"
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    LOGGER.addHandler(console)

    file_handler = logging.FileHandler(
        output_dir / "pipeline.log", mode="w", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def timed(label: str, function):
    LOGGER.info("%s ...", label)
    start = time.perf_counter()
    value = function()
    elapsed = time.perf_counter() - start
    LOGGER.info("%s completed in %.2f seconds", label, elapsed)
    return value, round(elapsed, 3)


def write_plan(dataframe: DataFrame, path: Path) -> None:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        dataframe.explain(mode="formatted")
        dataframe.explain(mode="extended")
    path.write_text(buffer.getvalue(), encoding="utf-8")


def benchmark(label: str, function, repeats: int) -> dict[str, object]:
    """Run an action several times and report every run plus the median."""
    runs = []
    for attempt in range(1, repeats + 1):
        start = time.perf_counter()
        function()
        runs.append(round(time.perf_counter() - start, 3))
        LOGGER.info("%s run %d/%d: %.3f seconds", label, attempt, repeats, runs[-1])
    median = round(statistics.median(runs), 3)
    LOGGER.info("%s median over %d runs: %.3f seconds", label, repeats, median)
    return {"runs": runs, "median": median}


def reset_output(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def frame_stats(metrics: DataFrame) -> dict[str, object]:
    row = metrics.agg(
        F.sum("trip_count").alias("clean_trip_count"),
        F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
        F.min("pickup_date").cast("string").alias("min_pickup_date"),
        F.max("pickup_date").cast("string").alias("max_pickup_date"),
    ).first()
    return row.asDict(recursive=True)


def top_zone_rows(spark: SparkSession, path: Path) -> list[dict[str, object]]:
    return [
        row.asDict(recursive=True)
        for row in spark.read.parquet(str(path)).limit(10).collect()
    ]


def run_pipeline(config: PipelineConfig) -> dict[str, object]:
    output_dir = config.output_dir
    configure_logging(output_dir)
    plan_dir = output_dir / "plans"
    result_dir = output_dir / "results"
    plan_dir.mkdir(parents=True, exist_ok=True)
    result_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "spark-events").mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = build_spark_session(
        master=config.master,
        shuffle_partitions=config.shuffle_partitions,
        event_log_dir=str(output_dir / "spark-events"),
    )
    spark.sparkContext.setLogLevel("WARN")

    timings: dict[str, float] = {}
    manifest: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "config": {
            **asdict(config),
            "output_dir": str(config.output_dir),
            "sample_fraction": config.sample_fraction,
        },
    }

    try:
        files = input_files(config.input_glob)
        source_bytes = compressed_size_bytes(config.input_glob)
        manifest["source"] = {
            "pattern": config.input_glob,
            "files": len(files),
            "compressed_bytes": source_bytes,
            "compressed_gib": round(source_bytes / (1024**3), 3),
        }
        LOGGER.info(
            "Source: %d Parquet files, %.3f GiB compressed",
            len(files),
            source_bytes / (1024**3),
        )

        raw = read_trips(spark, config.input_glob)
        zones = read_zones(spark, config.zone_lookup)
        transformed = add_trip_measures(apply_early_filters(select_source_columns(raw)))
        write_plan(transformed, plan_dir / "source_scan_plan.txt")

        if not 0 < config.sample_fraction <= 1:
            raise ValueError("sample_fraction must be in (0, 1]")
        if config.sample_fraction < 1:
            transformed = transformed.sample(
                withReplacement=False,
                fraction=config.sample_fraction,
                seed=42,
            )

        enriched = join_zone_lookup(transformed, zones)
        metrics = build_zone_hour_metrics(enriched)
        write_plan(metrics, plan_dir / "optimized_pipeline_plan.txt")

        metrics_path = result_dir / "zone_hour_metrics.parquet"
        reset_output(metrics_path)
        _, timings["zone_hour_metrics_write"] = timed(
            "Building and writing zone-hour metrics",
            lambda: metrics.repartition("pickup_month")
            .write.mode("overwrite")
            .partitionBy("pickup_month")
            .parquet(str(metrics_path)),
        )

        metrics_saved = spark.read.parquet(str(metrics_path))
        metrics_saved.createOrReplaceTempView("zone_hour_metrics")

        top_query = spark.sql(TOP_EVENING_ZONES_SQL)
        daily_query = spark.sql(DAILY_BOROUGH_SQL)
        monthly_query = spark.sql(MONTHLY_HOURLY_DEMAND_SQL)
        cache_query = spark.sql(CACHE_BENCHMARK_SQL)
        write_plan(top_query, plan_dir / "top_evening_zones_plan.txt")
        write_plan(daily_query, plan_dir / "daily_borough_plan.txt")
        write_plan(monthly_query, plan_dir / "monthly_partition_pruning_plan.txt")
        write_plan(cache_query, plan_dir / "cache_benchmark_plan.txt")

        top_path = result_dir / "top_evening_zones.parquet"
        daily_path = result_dir / "daily_borough_metrics.parquet"
        monthly_path = result_dir / "monthly_hourly_demand.parquet"
        reset_output(top_path)
        reset_output(daily_path)
        reset_output(monthly_path)

        _, timings["top_evening_zones_output"] = timed(
            "Writing top evening zones",
            lambda: top_query.coalesce(1)
            .write.mode("overwrite")
            .parquet(str(top_path)),
        )
        _, timings["daily_borough_output"] = timed(
            "Writing daily borough metrics",
            lambda: daily_query.coalesce(1)
            .write.mode("overwrite")
            .parquet(str(daily_path)),
        )

        _, timings["monthly_hourly_demand_output"] = timed(
            "Writing partition-pruned monthly hourly demand",
            lambda: monthly_query.coalesce(1)
            .write.mode("overwrite")
            .parquet(str(monthly_path)),
        )

        if not config.skip_cache_benchmark:
            # Reports run before this point so the monthly query scans the
            # Parquet partition directly instead of the later cached relation.
            # One untimed warm-up keeps JVM/JIT startup out of the timed runs.
            cache_query.count()
            uncached = benchmark(
                "Full-table SQL without cache",
                lambda: spark.sql(CACHE_BENCHMARK_SQL).count(),
                config.benchmark_repeats,
            )
            metrics_saved.persist(StorageLevel.MEMORY_AND_DISK)
            metrics_row_count, timings["cache_materialize"] = timed(
                "Materializing zone-hour metrics cache",
                lambda: metrics_saved.count(),
            )
            cached = benchmark(
                "Full-table SQL from cache",
                lambda: spark.sql(CACHE_BENCHMARK_SQL).count(),
                config.benchmark_repeats,
            )
            timings["cache_query_uncached"] = uncached["median"]
            timings["cache_query_cached"] = cached["median"]
            manifest["cache_benchmark"] = {
                "repeats": config.benchmark_repeats,
                "uncached_runs_seconds": uncached["runs"],
                "cached_runs_seconds": cached["runs"],
                "uncached_median_seconds": uncached["median"],
                "cached_median_seconds": cached["median"],
                "materialize_seconds": timings["cache_materialize"],
            }

        stats, timings["metrics_summary_query"] = timed(
            "Summarizing analytical metrics",
            lambda: frame_stats(metrics_saved),
        )
        if config.skip_cache_benchmark:
            metrics_row_count = metrics_saved.count()
        stats["metrics_row_count"] = metrics_row_count
        manifest["data"] = stats
        manifest["top_evening_zones"] = top_zone_rows(spark, top_path)
        manifest["timings_seconds"] = timings
        manifest["outputs"] = {
            "zone_hour_metrics": str(metrics_path),
            "top_evening_zones": str(top_path),
            "daily_borough_metrics": str(daily_path),
            "monthly_hourly_demand": str(monthly_path),
            "plans": str(plan_dir),
        }

        manifest_path = output_dir / "run_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, default=str), encoding="utf-8"
        )

        LOGGER.info("Clean trips summarized: %s", f"{stats['clean_trip_count']:,}")
        LOGGER.info("Results: %s", result_dir)
        LOGGER.info("Manifest: %s", manifest_path)
        LOGGER.info("Spark UI: %s", spark.sparkContext.uiWebUrl)

        if config.ui_pause_seconds:
            LOGGER.info(
                "Keeping Spark UI alive for %d seconds for screenshot capture",
                config.ui_pause_seconds,
            )
            time.sleep(config.ui_pause_seconds)

        return manifest
    finally:
        spark.stop()


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-glob",
        default="data/raw/yellow_tripdata_*.parquet",
        help="Parquet file glob for TLC yellow-taxi source data",
    )
    parser.add_argument(
        "--zones",
        default="data/raw/taxi_zone_lookup.csv",
        help="Path to the TLC taxi-zone CSV",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("artifacts/run")
    )
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Deterministic sample fraction for a faster smoke run",
    )
    parser.add_argument(
        "--skip-cache-benchmark",
        action="store_true",
        help="Skip the uncached/cached SQL comparison",
    )
    parser.add_argument(
        "--ui-pause-seconds",
        type=int,
        default=0,
        help="Keep the app and Spark UI alive after the run",
    )
    parser.add_argument(
        "--benchmark-repeats",
        type=int,
        default=5,
        help="Timed runs per side of the cache benchmark (median is reported)",
    )
    args = parser.parse_args()
    return PipelineConfig(
        input_glob=args.input_glob,
        zone_lookup=args.zones,
        output_dir=args.output_dir,
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        sample_fraction=args.sample_fraction,
        ui_pause_seconds=args.ui_pause_seconds,
        skip_cache_benchmark=args.skip_cache_benchmark,
        benchmark_repeats=args.benchmark_repeats,
    )


def main() -> int:
    run_pipeline(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
