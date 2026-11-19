"""Spark SQL analytics over the zone-hour intermediate."""

from __future__ import annotations

TOP_EVENING_ZONES_SQL = """
SELECT
    pickup_borough,
    pickup_zone,
    SUM(trip_count) AS total_trips,
    ROUND(SUM(gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(gross_revenue) / SUM(trip_count), 2) AS revenue_per_trip,
    ROUND(AVG(avg_distance_miles), 2) AS avg_distance_miles,
    ROUND(100 * SUM(tip_revenue) / SUM(fare_revenue), 2) AS tip_pct_of_fare
FROM zone_hour_metrics
WHERE pickup_hour BETWEEN 17 AND 20
  AND pickup_zone <> 'Unknown'
GROUP BY pickup_borough, pickup_zone
ORDER BY gross_revenue DESC
LIMIT 20
"""

DAILY_BOROUGH_SQL = """
SELECT
    pickup_date,
    pickup_borough,
    SUM(trip_count) AS trip_count,
    ROUND(SUM(gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(airport_trips), 0) AS airport_trips,
    ROUND(SUM(trip_count * avg_duration_minutes) / SUM(trip_count), 2)
        AS weighted_avg_duration_minutes,
    ROUND(100 * SUM(tip_revenue) / SUM(fare_revenue), 2) AS tip_pct_of_fare
FROM zone_hour_metrics
WHERE pickup_date >= DATE '2023-05-01'
  AND pickup_date < DATE '2025-01-01'
GROUP BY pickup_date, pickup_borough
ORDER BY pickup_date, pickup_borough
"""

PARTITION_PRUNED_MONTH = "2024-07"

# Filters on the partition column, so Spark reads only one of the 20
# pickup_month=... directories. The plan shows this under PartitionFilters.
MONTHLY_HOURLY_DEMAND_SQL = f"""
SELECT
    pickup_month,
    pickup_borough,
    pickup_hour,
    SUM(trip_count) AS trip_count,
    ROUND(SUM(gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(gross_revenue) / SUM(trip_count), 2) AS revenue_per_trip
FROM zone_hour_metrics
WHERE pickup_month = '{PARTITION_PRUNED_MONTH}'
GROUP BY pickup_month, pickup_borough, pickup_hour
ORDER BY pickup_borough, pickup_hour
"""

CACHE_BENCHMARK_SQL = """
SELECT
    pickup_month,
    pickup_borough,
    pickup_hour,
    SUM(trip_count) AS trip_count,
    ROUND(SUM(gross_revenue), 2) AS gross_revenue
FROM zone_hour_metrics
GROUP BY pickup_month, pickup_borough, pickup_hour
"""
