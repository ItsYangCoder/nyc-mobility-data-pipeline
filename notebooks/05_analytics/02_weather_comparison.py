# Databricks notebook source

# COMMAND ----------

# # Question 2 · How do demand and trip behavior differ by weather?
# Aggregate trips to pickup hour before matching the single-location weather observation. Results show association, not causation; precipitation is averaged once per weather hour. Distance means exclude flagged outliers; trip counts retain them.

# COMMAND ----------

display(spark.sql("""
WITH taxi_hours AS (
  SELECT weather_hour,
         COUNT(*) AS trip_count,
         SUM(trip_duration_minutes) AS duration_sum,
         COUNT(trip_duration_minutes) AS duration_count,
         SUM(valid_trip_distance) AS distance_sum,
         COUNT_IF(is_distance_outlier) AS flagged_distance_trips,
         COUNT(valid_trip_distance) AS distance_count,
         SUM(CASE WHEN fare_amount >= 0 THEN fare_amount END) AS nonnegative_fare_sum,
         COUNT_IF(fare_amount >= 0) AS nonnegative_fare_count,
         SUM(total_amount) AS total_amount_sum,
         COUNT(total_amount) AS total_amount_count
  FROM nyc_mobility.nyc_gold.fact_taxi_trip
  GROUP BY weather_hour
), aligned AS (
  SELECT t.*, w.temperature_2m, w.precipitation,
         CASE WHEN w.observation_hour IS NULL OR w.precipitation IS NULL THEN 'unknown'
              WHEN w.precipitation > 0 THEN 'wet' ELSE 'dry' END AS weather_condition
  FROM taxi_hours t
  LEFT JOIN nyc_mobility.nyc_gold.dim_weather_hour w
    ON t.weather_hour = w.observation_hour
)
SELECT weather_condition, COUNT(*) AS pickup_hours,
       SUM(trip_count) AS trip_count,
       SUM(flagged_distance_trips) AS flagged_distance_trips,
       ROUND(SUM(trip_count) / COUNT(*), 2) AS avg_trips_per_pickup_hour,
       ROUND(SUM(duration_sum) / NULLIF(SUM(duration_count), 0), 2) AS avg_trip_duration_minutes,
       ROUND(SUM(distance_sum) / NULLIF(SUM(distance_count), 0), 2) AS avg_trip_distance,
       ROUND(SUM(nonnegative_fare_sum) / NULLIF(SUM(nonnegative_fare_count), 0), 2) AS avg_nonnegative_fare,
       ROUND(SUM(total_amount_sum) / NULLIF(SUM(total_amount_count), 0), 2) AS avg_total_amount,
       ROUND(AVG(temperature_2m), 2) AS avg_hourly_temperature_c,
       ROUND(AVG(precipitation), 2) AS avg_hourly_precipitation_mm
FROM aligned
GROUP BY weather_condition
ORDER BY weather_condition
"""))
