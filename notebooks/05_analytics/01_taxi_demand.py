# Databricks notebook source

# COMMAND ----------

# # Question 1 · When and where is Taxi demand highest?
# Pickup grain: date + hour + zone. Higher trip count means more observed Green Taxi pickups, not all NYC trips.
# The Gold outlier flag excludes extreme source distances from distance metrics; all trips remain in demand counts. Review the configured threshold before presenting conclusions.

# COMMAND ----------

display(spark.sql("""
SELECT d.full_date, d.day_name, h.hour, z.borough, z.zone,
       COUNT(*) AS trip_count,
       COUNT_IF(t.is_distance_outlier) AS flagged_distance_trips,
       ROUND(SUM(t.valid_trip_distance), 2) AS total_distance,
       ROUND(AVG(t.valid_trip_distance), 2) AS avg_trip_distance,
       ROUND(SUM(CASE WHEN t.fare_amount >= 0 THEN t.fare_amount END), 2) AS nonnegative_fare_total
FROM nyc_mobility.nyc_gold.fact_taxi_trip t
JOIN nyc_mobility.nyc_gold.dim_date d ON t.pickup_date_key = d.date_key
JOIN nyc_mobility.nyc_gold.dim_hour h ON t.pickup_hour_key = h.hour_key
JOIN nyc_mobility.nyc_gold.dim_zone z ON t.pickup_location_id = z.location_id
GROUP BY d.full_date, d.day_name, h.hour, z.borough, z.zone
ORDER BY trip_count DESC, d.full_date, h.hour
LIMIT 100
"""))
