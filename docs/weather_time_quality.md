# Weather time quality finding (March–May 2026)

The Databricks run on 2026-09-24 loaded three monthly Weather JSON files into `nyc_mobility.nyc_bronze.weather_raw`: three Bronze records, one JSON object per month. The Silver view `nyc_mobility.nyc_silver.vw_weather_hourly_clean` expanded them to 2,208 distinct hourly timestamps from 2026-03-01 00:00 through 2026-05-31 23:00. The quality check reported zero missing timestamps and zero missing temperatures. Taxi Zones Silver has 265 rows, 265 unique location IDs and zero invalid zones.

## Source time observation

The March JSON declares `timezone=America/New_York`, while the March 8 Weather data contains 24 local clock labels, including `2026-03-08 02:00`. In 2026, the New York clock advances from 02:00 to 03:00 on March 8. A separate check of `vw_green_taxi_clean` found **zero taxi pickups** from 02:00 through 02:59 that day. Thus the extra Weather label does not currently match a Taxi pickup in that hour.

The source uses naive ISO-8601 hourly strings; a `TIMESTAMP_NTZ` alone does not establish a unique UTC instant. Treat the declared timezone and hourly labels as a **source-time inconsistency to review**, not as grounds for rewriting Bronze or silently deleting Silver rows. Do not claim the Weather source is fully timezone-correct based on the row and uniqueness checks alone.

## Before publishing the Gold join

- Reconcile Taxi pickup zones with `vw_taxi_zones_clean` on `pickup_zone_id = location_id`, and dropoff zones analogously.
- Reconcile pickup hours with `vw_weather_hourly_clean.observation_hour`; report unmatched Taxi rides and check whether the Weather hour key is unique.
- Keep the March 8 finding visible in the quality report. If the project later needs UTC event times or hourly DST accuracy, confirm the intended timezone convention from the source owner and define an explicit conversion policy.
- Rerun these checks when the source data or time-window parameters change.

Reference: [NIST Daylight Saving Time Rules](https://www.nist.gov/pml/time-and-frequency-division/popular-links/daylight-saving-time-dst) (2026 transition on March 8 at 2 a.m.).
