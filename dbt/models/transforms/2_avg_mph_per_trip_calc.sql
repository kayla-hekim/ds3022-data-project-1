{{ config(materialized='table') }}


WITH trips AS (
  SELECT *
  FROM stg_aggre_yellow_green
),


-- calculate avg mph per trip
with_hours AS (
  SELECT
    *,
    DATE_DIFF('second', pickup_ts, dropoff_ts) / 3600.0 AS total_hours
  FROM trips
)
SELECT
  with_hours.*,
  CASE
    WHEN total_hours > 0 THEN trip_distance_mi / total_hours
    ELSE NULL
  END AS mph_per_trip
FROM with_hours