{{ config(materialized='table') }}

{% set years = range(2015, 2025) %}
{% set colors = ['yellow', 'green'] %}
{% set raw_schema = 'main' %}

{% set selects = [] %}

{% for color in colors %}
    {% for year in years %}
        {% set table_name = color ~ '_' ~ year %}
        {% set fqtn = raw_schema ~ '.' ~ table_name %}

        -- standardizing more column names (especially pickup dropoff locations) - yellow && green
        {% do selects.append(
            "SELECT
                '" ~ color ~ "_taxi' AS taxi_color,
                " ~ (
                    "TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
                    TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                    if color == 'yellow' else
                    "TRY_CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
                    TRY_CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                ) ~ "
                TRY_CAST(trip_distance   AS DOUBLE) AS trip_distance_mi,
                TRY_CAST(passenger_count AS INT) AS passenger_count
            FROM " ~ fqtn
        ) %}
    {% endfor %}
{% endfor %}


WITH yellow_green_stg AS (
  {{ selects | join('\nUNION ALL\n') }}
)

SELECT * FROM yellow_green_stg