from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.analytics import TOP_EVENING_ZONES_SQL
from taxi_pipeline.spark_session import build_spark_session
from taxi_pipeline.transform import (
    add_trip_measures,
    apply_early_filters,
    build_zone_hour_metrics,
    join_zone_lookup,
    select_source_columns,
)


@pytest.fixture(scope="session")
def spark():
    session = build_spark_session(
        app_name="Taxi Pipeline Tests",
        shuffle_partitions=4,
        event_log_dir="artifacts/test-spark-events",
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def source_frame(spark: SparkSession):
    rows = [
        Row(
            tpep_pickup_datetime="2023-06-01 17:00:00",
            tpep_dropoff_datetime="2023-06-01 17:20:00",
            passenger_count=1,
            trip_distance=2.0,
            RatecodeID=1,
            PULocationID=1,
            DOLocationID=2,
            payment_type=1,
            fare_amount=10.0,
            tip_amount=3.0,
            total_amount=15.0,
        ),
        Row(
            tpep_pickup_datetime="2024-07-01 18:00:00",
            tpep_dropoff_datetime="2024-07-01 18:30:00",
            passenger_count=2,
            trip_distance=5.0,
            RatecodeID=2,
            PULocationID=2,
            DOLocationID=1,
            payment_type=1,
            fare_amount=20.0,
            tip_amount=4.0,
            total_amount=25.0,
        ),
        Row(
            tpep_pickup_datetime="2024-07-01 19:00:00",
            tpep_dropoff_datetime="2024-07-01 19:10:00",
            passenger_count=0,
            trip_distance=1.0,
            RatecodeID=1,
            PULocationID=1,
            DOLocationID=2,
            payment_type=1,
            fare_amount=8.0,
            tip_amount=1.0,
            total_amount=10.0,
        ),
        Row(
            tpep_pickup_datetime="2022-07-01 19:00:00",
            tpep_dropoff_datetime="2022-07-01 19:10:00",
            passenger_count=1,
            trip_distance=1.0,
            RatecodeID=1,
            PULocationID=1,
            DOLocationID=2,
            payment_type=1,
            fare_amount=8.0,
            tip_amount=1.0,
            total_amount=10.0,
        ),
        Row(
            tpep_pickup_datetime="2024-08-01 12:00:00",
            tpep_dropoff_datetime="2024-08-01 16:00:00",
            passenger_count=1,
            trip_distance=10.0,
            RatecodeID=1,
            PULocationID=1,
            DOLocationID=2,
            payment_type=1,
            fare_amount=30.0,
            tip_amount=5.0,
            total_amount=38.0,
        ),
    ]
    return spark.createDataFrame(rows).select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias(
            "tpep_pickup_datetime"
        ),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias(
            "tpep_dropoff_datetime"
        ),
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )


def zone_frame(spark: SparkSession):
    return spark.createDataFrame(
        [
            Row(
                pickup_location_id=1,
                pickup_borough="Manhattan",
                pickup_zone="Midtown",
                pickup_service_zone="Yellow Zone",
            ),
            Row(
                pickup_location_id=2,
                pickup_borough="Queens",
                pickup_zone="JFK",
                pickup_service_zone="Airports",
            ),
        ]
    )


def test_quality_filters_and_measures(spark):
    clean = add_trip_measures(
        apply_early_filters(select_source_columns(source_frame(spark)))
    )

    assert clean.count() == 3
    valid_rows = clean.orderBy("pickup_ts").collect()
    assert valid_rows[0].pickup_date.isoformat() == "2023-06-01"
    assert valid_rows[0].avg_speed_mph == 6.0
    assert valid_rows[1].is_airport_trip == 1


def test_zone_metrics_and_sql(spark):
    clean = add_trip_measures(
        apply_early_filters(select_source_columns(source_frame(spark)))
    )
    enriched = join_zone_lookup(clean, zone_frame(spark))
    metrics = build_zone_hour_metrics(enriched)

    assert metrics.count() == 3
    metrics.createOrReplaceTempView("zone_hour_metrics")
    rows = spark.sql(TOP_EVENING_ZONES_SQL).collect()

    assert len(rows) == 2
    assert rows[0].pickup_zone == "JFK"
    assert rows[0].gross_revenue == 25.0
