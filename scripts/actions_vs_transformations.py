#!/usr/bin/env python3
"""Demonstrate Spark's lazy transformations and eager actions."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import functions as F

from taxi_pipeline.spark_session import build_spark_session


def configure_logging() -> logging.Logger:
    Path("artifacts").mkdir(exist_ok=True)
    logger = logging.getLogger("lazy_demo")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    file_handler = logging.FileHandler(
        "artifacts/actions_vs_transformations.log",
        mode="w",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def main() -> int:
    logger = configure_logging()
    spark = build_spark_session(
        app_name="Actions vs Transformations Demo",
        event_log_dir="artifacts/spark-events",
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        tracker = spark.sparkContext.statusTracker()
        jobs_before = len(tracker.getJobIdsForGroup())
        logger.info("Jobs before building the plan: %d", jobs_before)
        logger.info(
            "Building a chain of transformations "
            "withColumn -> filter -> select ..."
        )

        transformed = (
            spark.range(0, 5_000_000, numPartitions=8)
            .withColumn("value", F.col("id") * 2)
            .filter(F.col("value") % 10 == 0)
            .select("id", "value")
        )

        jobs_after_build = len(tracker.getJobIdsForGroup())
        logger.info("Jobs after building the plan: %d", jobs_after_build)
        logger.info(
            "No data was read because transformations only update the logical plan."
        )
        logger.info("\nPhysical plan before an action:")
        transformed.explain(mode="formatted")

        logger.info("\nCalling take(5) to trigger an eager action ...")
        rows = transformed.take(5)
        jobs_after_action = len(tracker.getJobIdsForGroup())
        logger.info("Rows returned by take(5): %s", rows)
        logger.info(
            "Jobs after the action: %d (jobs launched: %d)",
            jobs_after_action,
            jobs_after_action - jobs_before,
        )
        logger.info(
            "A Spark job runs only when an action such as take, show, "
            "count, write, or collect requests a result."
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
