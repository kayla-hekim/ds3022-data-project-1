{{ config(materialized='table') }}

WITH vehicle_emissions_stg AS (

    SELECT
        -- adding the columms - adding all since there's only 2 excluded 
        -- it'd be interesting later to have vehicle type insights
        CAST(vehicle_type       AS VARCHAR) AS vehicle_type,
        CAST(fuel_type          AS VARCHAR) AS fuel_type,
        CAST(mpg_city AS BIGINT) AS mpg_city,
        CAST(mpg_highway AS BIGINT) AS mpg_highway,
        CAST(co2_grams_per_mile AS BIGINT) AS co2_grams_per_mile,
        CAST(vehicle_year_avg AS BIGINT) AS vehicle_year_avg

     FROM vehicle_emissions
)

SELECT * FROM vehicle_emissions_stg