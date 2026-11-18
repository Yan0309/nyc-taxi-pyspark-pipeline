"""Spark session configuration."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str = "NYC Taxi Distributed Processing Pipeline",
    master: str = "local[*]",
    shuffle_partitions: int = 48,
    event_log_dir: str = "artifacts/spark-events",
) -> SparkSession:
    """Build a session with AQE, Parquet pushdown, and local UI enabled."""
    Path(event_log_dir).mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
        .getOrCreate()
    )
