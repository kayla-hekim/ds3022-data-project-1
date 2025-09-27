{{ config(materialized='table') }}



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
        END AS mph_per_trip
    FROM with_hours
),



-- mph_calc, trip_hour, trip_day_of_week, week_number, trip_month_num
time_features AS (
    SELECT
        mph_calc.*, -- mph per trip (above)
        EXTRACT(HOUR FROM pickup_ts) AS trip_hour, -- calculate trip hour
        STRFTIME(pickup_ts, '%A') AS trip_day_of_week, -- calculate trip day of week
        EXTRACT(WEEK FROM pickup_ts) AS week_number, -- calculate week number
        EXTRACT(MONTH FROM pickup_ts) AS trip_month_num, -- calculate month
        CASE lower(taxi_color)
            WHEN 'yellow_taxi' THEN 'yellow_taxi'
            WHEN 'green_taxi'  THEN 'green_taxi'
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
        time_features.trip_distance_mi * co2_factors.co2_kg_per_mile AS co2_kg_per_trip
    FROM time_features
    LEFT JOIN co2_factors USING (vehicle_type)
)



SELECT 
    pickup_ts,
    dropoff_ts,
    trip_distance_mi,
    passenger_count,
    fare_amount,
    total_amount,
    pickup_location_id,
    dropoff_location_id,
    vendor_id,
    payment_type,
    trip_hour,
    trip_day_of_week,
    week_number,
    trip_month_num,
    mph_per_trip,
    co2_kg_per_trip,
    vehicle_type
    -- *
FROM with_co2_factors