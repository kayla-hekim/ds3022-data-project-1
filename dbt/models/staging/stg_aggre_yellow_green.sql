{{ config(materialized='table') }}

{% set months = range(1, 13) %}
{% set years = [2024] %}
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
                    "CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                    if color == 'yellow' else
                    "CAST(lpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
                    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                ) ~ "
                CAST(trip_distance   AS DOUBLE) AS trip_distance_mi,
                CAST(passenger_count AS INT)    AS passenger_count
            FROM " ~ fqtn
        ) %}
    {% endfor %}
{% endfor %}


WITH yellow_green_stg AS (
  {{ selects | join('\nUNION ALL\n') }}
)

SELECT * FROM yellow_green_stg