import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)


def single_largest_carbon_trip_year(color, years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for largest CO2 trip: color={color}, years={list(years)}")

        largest_trip = ""
        largest_co2 = -1.0
        color_lower = color.lower()

        start_year = min(years)
        end_year = max(years) + 1

        for year in years:
            table_name = f"{color_lower}_{year}"

            if color_lower.lower() == 'yellow':
                sql_query = f"""
                    SELECT
                        pickup_ts,
                        dropoff_ts,
                        trip_distance_mi,
                        passenger_count,
                        hour_of_day,
                        day_of_week,
                        week_of_year,
                        month_of_year,
                        avg_mph,
                        trip_co2_kgs,
                        vehicle_type
                    FROM data_transformation
                    WHERE vehicle_type = '{color_lower}_taxi'
                        AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                        AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                    ORDER BY trip_co2_kgs DESC, trip_distance_mi DESC, dropoff_ts ASC
                    LIMIT 1
                """
            
            elif color_lower == 'green':
                sql_query = f"""
                    SELECT
                        pickup_ts,
                        dropoff_ts,
                        trip_distance_mi,
                        passenger_count,
                        hour_of_day,
                        day_of_week,
                        week_of_year,
                        month_of_year,
                        avg_mph,
                        trip_co2_kgs,
                        vehicle_type
                    FROM data_transformation
                    WHERE vehicle_type = '{color_lower}_taxi'
                        AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                        AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                    ORDER BY trip_co2_kgs DESC, trip_distance_mi DESC, dropoff_ts ASC
                    LIMIT 1
                """

            else:
                continue

            try:
                row = con.execute(sql_query).fetchone()
            except Exception as e:
                logger.warning(f"Skipping {table_name}: {e}")
                continue

            if row:
                trip_co2 = row[9]
                if trip_co2 is not None and trip_co2 > largest_co2:
                    largest_co2 = trip_co2
                    largest_trip = {
                        "pickup_ts": row[0],
                        "dropoff_ts": row[1],
                        "trip_distance_mi": row[2],
                        "passenger_count": row[3],
                        "hour_of_day": row[4],
                        "day_of_week": row[5],
                        "week_of_year": row[6],
                        "month_of_year": row[7],
                        "avg_mph": row[8],
                        "trip_co2_kgs": row[9],
                        "vehicle_type": row[10],
                        "year": year,
                    }

        return largest_trip

    except Exception as e:
        print(f"Error in finding single largest carbon trip in the year in years={list(years)} for {color_lower}: {e}")
        logger.error(f"Error in finding single largest carbon trip in the year in years={list(years)} for {color_lower}: {e}")
        return None

    finally:
        if con:
            con.close()

def pretty_print_largest_carbon_trip(color, largest_carbon, years=range(2024, 2025)):
    start_year = min(years)
    end_year = max(years)

    if not largest_carbon:
        print(f"FOR {color.upper()}, YEAR RANGE {start_year}-{end_year}: no result found")
        return
    
    print(f"FOR {color.upper()}, YEAR RANGE {start_year}-{end_year}:")

    for key, value in largest_carbon.items():
        print(f" - {key}: {value},")



# Call all methods from analysis.py here
if __name__ == "__main__":
    years = range(2024, 2025)

    # SINGLE LARGEST CARBON TRIP OF THE YEARS - YELLOW THEN GREEN:
    yellow_largest_carbon = single_largest_carbon_trip_year('yellow')
    green_largest_carbon = single_largest_carbon_trip_year('green')

    pretty_print_largest_carbon_trip("yellow", yellow_largest_carbon, years)

    print("\n")

    pretty_print_largest_carbon_trip("green", green_largest_carbon, years)




# ## Analyze

# Complete the `analysis.py` script to report the following calculations using DuckDB/SQL. You should give one answer for each cab type, YELLOW and GREEN:

# 1. What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
# 2. Across the entire year, what on average are the most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips? (1-24)
# 3. Across the entire year, what on average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips? (Sun-Sat)
# 4. Across the entire year, what on average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips? (1-52)
# 5. Across the entire year, what on average are the most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips? (Jan-Dec)
# 6. Use a plotting library of your choice (`matplotlib`, `seaborn`, etc.) to generate a time-series plot or histogram with MONTH
# along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals.

# Your script should give text outputs for each calculation WITH a label explaining the value. The plot should be output as a PNG/JPG/GIF image 
# committed within your project.
