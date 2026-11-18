"""Lazy transformations for the taxi pipeline."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def select_source_columns(trips: DataFrame) -> DataFrame:
    """Prune unused Parquet columns before any row-level processing."""
    return trips.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )


def apply_early_filters(
    trips: DataFrame,
    start_date: str = "2023-05-01",
    end_date: str = "2025-01-01",
) -> DataFrame:
    """Apply selective filters before the zone join or any aggregation."""
    start_ts = F.lit(start_date).cast("timestamp")
    end_ts = F.lit(end_date).cast("timestamp")

    return trips.filter(
        (F.col("pickup_ts") >= start_ts) & (F.col("pickup_ts") < end_ts)
    ).filter(
        (F.col("dropoff_ts") > F.col("pickup_ts"))
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 100)
    ).filter(
        (F.col("fare_amount") > 0)
        & (F.col("total_amount") > 0)
        & (F.col("tip_amount") >= 0)
    ).filter(
        F.col("passenger_count").isNull()
        | F.col("passenger_count").between(1, 6)
    )


def add_trip_measures(trips: DataFrame) -> DataFrame:
    """Add reusable trip measures using withColumn."""
    duration_minutes = (
        F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")
    ) / F.lit(60.0)

    return (
        trips.withColumn("trip_duration_minutes", duration_minutes)
        .filter(F.col("trip_duration_minutes").between(1, 240))
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.round(
                    F.col("trip_distance")
                    / (F.col("trip_duration_minutes") / F.lit(60.0)),
                    2,
                ),
            ),
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn("pickup_weekday", F.date_format("pickup_ts", "E"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "is_airport_trip",
            F.when(F.col("rate_code_id").isin(2, 3), F.lit(1)).otherwise(F.lit(0)),
        )
    )


def join_zone_lookup(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Broadcast the tiny lookup table to avoid a shuffle join."""
    joined = trips.join(
        F.broadcast(
            zones.select(
                "pickup_location_id",
                "pickup_borough",
                "pickup_zone",
                "pickup_service_zone",
            )
        ),
        on="pickup_location_id",
        how="left",
    )
    return joined.withColumn(
        "pickup_borough", F.coalesce(F.col("pickup_borough"), F.lit("Unknown"))
    ).withColumn(
        "pickup_zone", F.coalesce(F.col("pickup_zone"), F.lit("Unknown"))
    ).withColumn(
        "pickup_service_zone",
        F.coalesce(F.col("pickup_service_zone"), F.lit("Unknown")),
    )


def build_zone_hour_metrics(trips: DataFrame) -> DataFrame:
    """Create a compact analytical product for repeated SQL queries."""
    return (
        trips.groupBy(
            "pickup_month",
            "pickup_date",
            "pickup_weekday",
            "pickup_hour",
            "pickup_borough",
            "pickup_zone",
            "pickup_service_zone",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("fare_amount"), 2).alias("fare_revenue"),
            F.round(F.sum("tip_amount"), 2).alias("tip_revenue"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"), 2).alias(
                "avg_duration_minutes"
            ),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
            F.sum("is_airport_trip").alias("airport_trips"),
        )
        .withColumn(
            "avg_revenue_per_trip",
            F.round(F.col("gross_revenue") / F.col("trip_count"), 2),
        )
        .withColumn(
            "tip_pct_of_fare",
            F.when(
                F.col("fare_revenue") > 0,
                F.round(
                    F.lit(100.0)
                    * F.col("tip_revenue")
                    / F.col("fare_revenue"),
                    2,
                ),
            ),
        )
    )
