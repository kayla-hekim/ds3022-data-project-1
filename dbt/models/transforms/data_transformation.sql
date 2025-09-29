{{ config(materialized='table', pre_hook="SET TimeZone='America/New_York';") }}



WITH trips AS (
    SELECT *
    FROM {{ ref('stg_aggre_yellow_green') }}
),



-- calculate avg mph per trip
with_hours AS (
    SELECT
        *,
        DATE_DIFF('second', pickup_ts, dropoff_ts) / 3600.0 AS total_hours
    FROM trips
),

mph_calc AS (
    SELECT
        with_hours.*,
        CASE
            WHEN total_hours > 0 THEN trip_distance_mi / total_hours
            ELSE NULL
        END AS avg_mph
    FROM with_hours
),



-- avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year
time_features AS (
    SELECT
        mph_calc.*, -- mph per trip (above)
        EXTRACT(YEAR FROM pickup_ts) AS trip_year, -- for using year later
        EXTRACT(HOUR FROM pickup_ts) AS hour_of_day, -- calculate trip hour
        STRFTIME(pickup_ts, '%A')    AS day_of_week, -- calculate trip day of week
        CAST(STRFTIME(CAST(pickup_ts AS DATE), '%V') AS INT) AS week_of_year, -- calculate week number
        EXTRACT(MONTH FROM CAST(pickup_ts AS DATE)) AS month_of_year, -- calculate month
        CASE lower(taxi_color)
            WHEN 'yellow_taxi' THEN 'yellow_taxi'
            WHEN 'green_taxi'  THEN 'green_taxi'
            ELSE NULL
        END AS vehicle_type -- use in co2_factors in with_co2_factors
    FROM mph_calc
),



-- Calculate cso2 per trip in kilograms
co2_factors AS (
    SELECT
        vehicle_type,
        co2_grams_per_mile / 1000.0 AS co2_kg_per_mile
    FROM {{ ref('stg_vehicle_emissions') }}
    WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
),
with_co2_factors AS (
    SELECT
        time_features.*,
        co2_factors.co2_kg_per_mile,
        time_features.trip_distance_mi * co2_factors.co2_kg_per_mile AS trip_co2_kgs
    FROM time_features
    LEFT JOIN co2_factors USING (vehicle_type)
)


-- only want certain columns
SELECT 
    pickup_ts,
    dropoff_ts,
    trip_distance_mi,
    passenger_count,
    
    trip_year,
    hour_of_day,
    day_of_week,
    week_of_year,
    month_of_year,
    avg_mph,
    trip_co2_kgs,
    vehicle_type
    -- *
FROM with_co2_factors