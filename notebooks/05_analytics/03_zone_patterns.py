# Databricks notebook source

# COMMAND ----------

# # Question 3 · Which zones show the strongest mobility patterns?
# Pickup and dropoff are separate roles; one trip appears once in each role. Weather is aligned to pickup hour, including for dropoff role. Distance averages exclude flagged distances while counts retain all trips.

# COMMAND ----------

display(spark.sql("""
WITH trip_roles AS (
  SELECT pickup_date_key AS date_key, pickup_hour_key AS hour_key, weather_hour,
         pickup_location_id AS location_id, 'pickup' AS zone_role,
         valid_trip_distance, is_distance_outlier, trip_duration_minutes, fare_amount
  FROM nyc_mobility.nyc_gold.fact_taxi_trip
  UNION ALL
  SELECT pickup_date_key, pickup_hour_key, weather_hour,
         dropoff_location_id, 'dropoff',
         valid_trip_distance, is_distance_outlier, trip_duration_minutes, fare_amount
  FROM nyc_mobility.nyc_gold.fact_taxi_trip
), aligned AS (
  SELECT t.*, z.borough, z.zone,
         CASE WHEN w.observation_hour IS NULL OR w.precipitation IS NULL THEN 'unknown'
              WHEN w.precipitation > 0 THEN 'wet' ELSE 'dry' END AS pickup_weather_condition
  FROM trip_roles t
  JOIN nyc_mobility.nyc_gold.dim_zone z ON t.location_id = z.location_id
  LEFT JOIN nyc_mobility.nyc_gold.dim_weather_hour w
    ON t.weather_hour = w.observation_hour
)
SELECT zone_role, borough, zone, pickup_weather_condition,
       COUNT(*) AS trip_count,
       COUNT_IF(is_distance_outlier) AS flagged_distance_trips,
       COUNT(DISTINCT date_key) AS active_pickup_dates,
       ROUND(AVG(valid_trip_distance), 2) AS avg_trip_distance,
       ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
       ROUND(AVG(CASE WHEN fare_amount >= 0 THEN fare_amount END), 2) AS avg_nonnegative_fare
FROM aligned
GROUP BY zone_role, borough, zone, pickup_weather_condition
ORDER BY trip_count DESC, zone_role, borough, zone
LIMIT 100
"""))

# COMMAND ----------

# ## How zone activity changes by time and weather
# Both roles use pickup day/hour and pickup-hour weather. The two roles each count a trip once.

# COMMAND ----------

display(spark.sql("""
WITH trip_roles AS (
  SELECT pickup_date_key AS date_key, pickup_hour_key AS hour_key, weather_hour,
         pickup_location_id AS location_id, 'pickup' AS zone_role
  FROM nyc_mobility.nyc_gold.fact_taxi_trip
  UNION ALL
  SELECT pickup_date_key, pickup_hour_key, weather_hour, dropoff_location_id, 'dropoff'
  FROM nyc_mobility.nyc_gold.fact_taxi_trip
)
SELECT t.zone_role, z.borough, z.zone, d.day_name AS pickup_day,
       t.hour_key AS pickup_hour,
       CASE WHEN w.observation_hour IS NULL OR w.precipitation IS NULL THEN 'unknown'
            WHEN w.precipitation > 0 THEN 'wet' ELSE 'dry' END AS pickup_weather_condition,
       COUNT(*) AS trip_count
FROM trip_roles t
JOIN nyc_mobility.nyc_gold.dim_zone z ON t.location_id = z.location_id
JOIN nyc_mobility.nyc_gold.dim_date d ON t.date_key = d.date_key
LEFT JOIN nyc_mobility.nyc_gold.dim_weather_hour w
  ON t.weather_hour = w.observation_hour
GROUP BY t.zone_role, z.borough, z.zone, d.day_name, t.hour_key,
         CASE WHEN w.observation_hour IS NULL OR w.precipitation IS NULL THEN 'unknown'
              WHEN w.precipitation > 0 THEN 'wet' ELSE 'dry' END
ORDER BY trip_count DESC, z.borough, z.zone
LIMIT 100
"""))
