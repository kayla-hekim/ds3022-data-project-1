{{ config(materialized='view') }}

{% set months = range(1, 13) %}
{% set years = [2024] %}
{% set colors = ['yellow', 'green'] %}
{% set raw_schema = 'main' %}


{% set selects = [] %}
{% for color in colors %}
    {% for year in years %}
        {% for month in months %}
            {% set table_name = color ~ '_' ~ year ~ '_' ~ ("%02d"|format(month)) %}
            {% set fqtn = raw_schema ~ '.' ~ table_name %} 
            -- standardizing more column names (especially pickup dropoff locations) - yellow && green
            {% do selects.append(
                "
                SELECT
                    '" ~ color ~ "_taxi' AS taxi_color,
                    " ~ (
                        "CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
                        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                        if color == 'yellow' else
                        "CAST(lpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
                        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,"
                    ) ~ "
                    CAST(trip_distance   AS DOUBLE) AS trip_distance_mi,
                    CAST(passenger_count AS INT)    AS passenger_count,
                    CAST(fare_amount     AS DOUBLE) AS fare_amount,
                    CAST(total_amount    AS DOUBLE) AS total_amount,
                    CAST(PULocationID    AS INT)    AS pickup_location_id,
                    CAST(DOLocationID    AS INT)    AS dropoff_location_id,
                    CAST(VendorID        AS INT)    AS vendor_id,
                    CAST(payment_type    AS INT)    AS payment_type
                FROM " ~ fqtn
            ) %}
        {% endfor %}
    {% endfor %}
{% endfor %}


WITH yellow_green_stg AS (
  {{ selects | join('\nUNION ALL\n') }}
)

SELECT * FROM yellow_green_stg