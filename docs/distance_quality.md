# Taxi distance quality

**Policy.** `gold_marts.py` defines `max_valid_distance_miles` (default: 100 miles). The Gold fact keeps every trip and its original `trip_distance`; `is_distance_outlier` flags negative values or values above the limit. Missing distances yield a null flag and fail Gold quality. The daily view and Q1–Q3 analytics read the fact's `valid_trip_distance` column for distance sums and averages, leaving flagged distances out. Trip counts, duration and fare measures still include those trips. The threshold is a provisional analytics choice, not a source correction. After changing it, rerun Gold, Gold quality, then analytics.

**Observed March–May 2026 run.** Gold quality reconciled 133,355 unique trips with Silver, 265 Zones and 2,208 Weather hours; dimension/Weather joins had zero misses or multiplied trips. It reported **31 flagged distances**. For May 11 at 08:00 in East Harlem North, Q1 kept 56 pickups, flagged one distance, and reported 95.64 total miles and 1.74 average miles. The 33,518.65-mile source value remains in the fact for investigation. Q2 retained all 133,355 trips and reconciled 28 dry plus 3 wet distance flags to 31.

Weather comparisons use one observation location and unequal dry/wet hour exposure; they describe associations, not causal effects.
