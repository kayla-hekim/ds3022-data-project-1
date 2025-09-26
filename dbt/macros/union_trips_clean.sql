-- macro - apparently can handle a non-existant table that's tried (error handling)
-- documentation: https://jinja.palletsprojects.com/en/stable/templates/#:~:text=loop%20filtering.-,Macros,-%C2%B6
{% macro union_trips_clean(colors, years, months) %}
    {% set selects = [] %}

    {% for color in colors %} -- loop over yellow then green
        {% for year in years %} -- loop over years (default 2024 only, else the other years too)
            {% for month in months %} -- loop over all 12 months each year

                {% set ident = color ~ '_' ~ year ~ '_' ~ ("%02d"|format(month)) ~ '_clean' %}
                {% set rel = adapter.get_relation(database=target.database, schema='tlc', identifier=ident) %}

                {% if rel %}
                    {% set sel %} -- populating sel for later selects
                        SELECT
                            '{{ color }}' AS taxi_color, -- creating new column of taxi's color in taxi_color from color in colors
                            {% if color == 'yellow' %}
                                -- making pickup & dropoff time standard - yellow
                                CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts, 
                                CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
                            {% else %}
                                -- making pickup & dropoff time standard - green
                                CAST(lpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
                                CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
                            {% endif %}

                            -- standardizing more column names (especially pickup dropoff locations) - yellow && green
                            CAST(trip_distance   AS DOUBLE) AS trip_distance_mi,
                            CAST(passenger_count AS INT)    AS passenger_count,
                            CAST(fare_amount     AS DOUBLE) AS fare_amount,
                            CAST(total_amount    AS DOUBLE) AS total_amount,
                            CAST(PULocationID    AS INT)    AS pickup_location_id,
                            CAST(DOLocationID    AS INT)    AS dropoff_location_id,
                            CAST(VendorID        AS INT)    AS vendor_id,
                            CAST(payment_type    AS INT)    AS payment_type

                        FROM {{ rel }}

                    {% endset %}
                    {% do selects.append(sel) %} -- add the new item read from existing tables into the set selection for later union
                {% endif %}
            {% endfor %}
        {% endfor %}
    {% endfor %}

    {% if selects | length == 0 %}
    -- if the tables aren't available/can't be put in the selects list (those that exist), assign NULL to all :(((
    SELECT
        CAST(NULL AS VARCHAR)   AS taxi_color,
        CAST(NULL AS TIMESTAMP) AS pickup_ts,
        CAST(NULL AS TIMESTAMP) AS dropoff_ts,
        CAST(NULL AS DOUBLE)    AS trip_distance_mi,
        CAST(NULL AS INT)       AS passenger_count,
        CAST(NULL AS DOUBLE)    AS fare_amount,
        CAST(NULL AS DOUBLE)    AS total_amount,
        CAST(NULL AS INT)       AS pickup_location_id,
        CAST(NULL AS INT)       AS dropoff_location_id,
        CAST(NULL AS INT)       AS vendor_id,
        CAST(NULL AS INT)       AS payment_type
    WHERE 1=0

    -- this is the unioning of all EXISTING tables
    {% else %}
        {{ selects | join('\nUNION ALL\n') }}
    {% endif %}

{% endmacro %}