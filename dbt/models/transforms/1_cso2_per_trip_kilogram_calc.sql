{{ config(materialized='table') }}


WITH trips AS (
    SELECT *
    FROM stg_aggre_yellow_green
),
vehicles AS (
    SELECT *
    FROM stg_vehicle_emissions
),


-- Calculate cso2 per trip in kilograms
co2_factors AS (
    SELECT
        vehicle_type,
        co2_grams_per_mile / 1000.0 AS co2_kg_per_mile
    FROM stg_vehicle_emissions
    WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
)
SELECT
    trips.*,
    co2_factors.co2_kg_per_mile,
    trips.trip_distance_mi * co2_factors.co2_kg_per_mile AS co2_kg_per_trip
FROM trips
LEFT JOIN co2_factors
    ON co2_factors.vehicle_type = trips.taxi_color
