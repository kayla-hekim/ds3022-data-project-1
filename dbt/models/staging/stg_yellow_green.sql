{{ config(materialized='view') }}

-- we're looping over all yellow and green tables in emissions.duckdb
-- jinja template - using macros in case we have a missing table in parquet (see documentation in macros/union_trips_clean.sql)


-- Here's where things are actually executed (looping over months, years, and colors, passed into that macro in the WITH)
{% set months = range(1, 13) %}
{% set years = [2024] %}
{% set colors = ['yellow', 'green'] %}

WITH trips AS (
  {{ union_trips_clean(colors, years, months) }} -- run macro above
)

SELECT * FROM trips