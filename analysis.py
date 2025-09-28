import duckdb
import logging
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


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
                        AND trip_co2_kgs IS NOT NULL
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
                        AND trip_co2_kgs IS NOT NULL
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



def carbon_heavy_light_hour (years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        start_year = min(years)
        end_year = max(years) + 1
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for heavy and light carbon hours: years={list(years)}")

        result_yellow = None
        result_green = None

        result_yellow = con.execute(f""" 
            SELECT
                hour_of_day,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'yellow_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day;
        """).fetchall()

        result_green = con.execute(f"""
            SELECT
                hour_of_day,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'green_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day;
        """).fetchall()

        if not result_yellow or not result_green:
            logger.warning("One of the color result sets is empty.")
            return None

        yellow_hour_min = min(result_yellow, key=lambda x: x[1])
        yellow_hour_max = max(result_yellow, key=lambda x: x[1])

        green_hour_min = min(result_green, key=lambda x: x[1])
        green_hour_max = max(result_green, key=lambda x: x[1])

        return yellow_hour_min, yellow_hour_max, green_hour_min, green_hour_max

    
    except Exception as e:
        print(f"Error in finding the heavy and light carbon hours={list(years)}: {e}")
        logger.error(f"Error in finding the heavy and light carbon hours={list(years)}: {e}")
        return None

    finally:
        if con:
            con.close()



def carbon_heavy_light_DOW (years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for heavy and light carbon DOW: years={list(years)}")

        result_yellow = None
        result_green = None

        result_yellow = con.execute(f""" 
            SELECT
                CASE day_of_week
                    WHEN 'Sunday'    THEN 'Sun'
                    WHEN 'Monday'    THEN 'Mon'
                    WHEN 'Tuesday'   THEN 'Tue'
                    WHEN 'Wednesday' THEN 'Wed'
                    WHEN 'Thursday'  THEN 'Thu'
                    WHEN 'Friday'    THEN 'Fri'
                    WHEN 'Saturday'  THEN 'Sat'
                    ELSE day_of_week
                END AS dow_abbrev,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'yellow_taxi'
            GROUP BY 1
            ORDER BY CASE
                WHEN dow_abbrev='Sun' THEN 1
                WHEN dow_abbrev='Mon' THEN 2
                WHEN dow_abbrev='Tue' THEN 3
                WHEN dow_abbrev='Wed' THEN 4
                WHEN dow_abbrev='Thu' THEN 5
                WHEN dow_abbrev='Fri' THEN 6
                WHEN dow_abbrev='Sat' THEN 7
            END;
        """).fetchall()

        result_green = con.execute(f"""
            SELECT
                CASE day_of_week
                    WHEN 'Sunday'    THEN 'Sun'
                    WHEN 'Monday'    THEN 'Mon'
                    WHEN 'Tuesday'   THEN 'Tue'
                    WHEN 'Wednesday' THEN 'Wed'
                    WHEN 'Thursday'  THEN 'Thu'
                    WHEN 'Friday'    THEN 'Fri'
                    WHEN 'Saturday'  THEN 'Sat'
                    ELSE day_of_week
                END AS dow_abbrev,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'green_taxi'
            GROUP BY 1
            ORDER BY CASE
                WHEN dow_abbrev='Sun' THEN 1
                WHEN dow_abbrev='Mon' THEN 2
                WHEN dow_abbrev='Tue' THEN 3
                WHEN dow_abbrev='Wed' THEN 4
                WHEN dow_abbrev='Thu' THEN 5
                WHEN dow_abbrev='Fri' THEN 6
                WHEN dow_abbrev='Sat' THEN 7
            END;
        """).fetchall()

        if not result_yellow or not result_green:
            logger.warning("One of the color result sets is empty.")
            return None

        yellow_DOW_min = min(result_yellow, key=lambda x: x[1])
        yellow_DOW_max = max(result_yellow, key=lambda x: x[1])

        green_DOW_min = min(result_green, key=lambda x: x[1])
        green_DOW_max = max(result_green, key=lambda x: x[1])

        return yellow_DOW_min, yellow_DOW_max, green_DOW_min, green_DOW_max

    except Exception as e:
        print(f"Error in finding the heavy and light carbon DOW={list(years)}: {e}")
        logger.error(f"Error in finding the heavy and light carbon DOW={list(years)}: {e}")
        return None

    finally:
        if con:
            con.close()



def carbon_heavy_light_week (years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        start_year = min(years)
        end_year = max(years) + 1
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for heavy and light carbon weeks: years={list(years)}")

        result_yellow = None
        result_green = None

        result_yellow = con.execute(f""" 
            SELECT
                week_of_year,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'yellow_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY week_of_year
            ORDER BY week_of_year;
        """).fetchall()

        result_green = con.execute(f"""
            SELECT
                week_of_year,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'green_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY week_of_year
            ORDER BY week_of_year;
        """).fetchall()

        if not result_yellow or not result_green:
            logger.warning("One of the color result sets is empty.")
            return None

        yellow_week_min = min(result_yellow, key=lambda x: x[1])
        yellow_week_max = max(result_yellow, key=lambda x: x[1])

        green_week_min = min(result_green, key=lambda x: x[1])
        green_week_max = max(result_green, key=lambda x: x[1])

        return yellow_week_min, yellow_week_max, green_week_min, green_week_max

    except Exception as e:
        print(f"Error in finding the heavy and light carbon weeks={list(years)}: {e}")
        logger.error(f"Error in finding the heavy and light carbon weeks={list(years)}: {e}")
        return None

    finally:
        if con:
            con.close()



def carbon_heavy_light_month (years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        start_year = min(years)
        end_year = max(years) + 1
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for heavy and light carbon months: years={list(years)}")

        result_yellow = None
        result_green = None

        result_yellow = con.execute(f""" 
            SELECT
                CASE month_of_year
                    WHEN 1 THEN 'Jan'
                    WHEN 2 THEN 'Feb'
                    WHEN 3 THEN 'Mar'
                    WHEN 4 THEN 'Apr'
                    WHEN 5 THEN 'May'
                    WHEN 6 THEN 'Jun'
                    WHEN 7 THEN 'Jul'
                    WHEN 8 THEN 'Aug'
                    WHEN 9 THEN 'Sep'
                    WHEN 10 THEN 'Oct'
                    WHEN 11 THEN 'Nov'
                    WHEN 12 THEN 'Dec'
                    ELSE CAST(month_of_year AS VARCHAR)
                END AS mo_abbrev,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'yellow_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY mo_abbrev
            ORDER BY CASE
                WHEN mo_abbrev='Jan' THEN 1
                WHEN mo_abbrev='Feb' THEN 2
                WHEN mo_abbrev='Mar' THEN 3
                WHEN mo_abbrev='Apr' THEN 4
                WHEN mo_abbrev='May' THEN 5
                WHEN mo_abbrev='Jun' THEN 6
                WHEN mo_abbrev='Jul' THEN 7
                WHEN mo_abbrev='Aug' THEN 8
                WHEN mo_abbrev='Sep' THEN 9
                WHEN mo_abbrev='Oct' THEN 10
                WHEN mo_abbrev='Nov' THEN 11
                WHEN mo_abbrev='Dec' THEN 12
            END;
        """).fetchall()

        result_green = con.execute(f"""
            SELECT
                CASE month_of_year
                    WHEN 1    THEN 'Jan'
                    WHEN 2    THEN 'Feb'
                    WHEN 3   THEN 'Mar'
                    WHEN 4 THEN 'Apr'
                    WHEN 5  THEN 'May'
                    WHEN 6    THEN 'Jun'
                    WHEN 7  THEN 'Jul'
                    WHEN 8  THEN 'Aug'
                    WHEN 9  THEN 'Sep'
                    WHEN 10  THEN 'Oct'
                    WHEN 11  THEN 'Nov'
                    WHEN 12  THEN 'Dec'
                    ELSE CAST(month_of_year AS VARCHAR)
                END AS mo_abbrev,
                AVG(trip_co2_kgs) AS avg_co2_per_trip
            FROM data_transformation
            WHERE vehicle_type = 'green_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY mo_abbrev
            ORDER BY CASE
                WHEN mo_abbrev='Jan' THEN 1
                WHEN mo_abbrev='Feb' THEN 2
                WHEN mo_abbrev='Mar' THEN 3
                WHEN mo_abbrev='Apr' THEN 4
                WHEN mo_abbrev='May' THEN 5
                WHEN mo_abbrev='Jun' THEN 6
                WHEN mo_abbrev='Jul' THEN 7
                WHEN mo_abbrev='Aug' THEN 8
                WHEN mo_abbrev='Sep' THEN 9
                WHEN mo_abbrev='Oct' THEN 10
                WHEN mo_abbrev='Nov' THEN 11
                WHEN mo_abbrev='Dec' THEN 12
            END;

        """).fetchall()

        if not result_yellow or not result_green:
            logger.warning("One of the color result sets is empty.")
            return None

        yellow_mo_min = min(result_yellow, key=lambda x: x[1])
        yellow_mo_max = max(result_yellow, key=lambda x: x[1])

        green_mo_min = min(result_green, key=lambda x: x[1])
        green_mo_max = max(result_green, key=lambda x: x[1])

        return yellow_mo_min, yellow_mo_max, green_mo_min, green_mo_max

    except Exception as e:
        print(f"Error in finding the heavy and light carbon months={list(years)}: {e}")
        logger.error(f"Error in finding the heavy and light carbon months={list(years)}: {e}")
        return None

    finally:
        if con:
            con.close()



def plot_co2_month_by_co2totals(years=range(2024, 2025), db_path='./emissions2024.duckdb'):
    con = None

    try:
        start_year = min(years)
        end_year = max(years) + 1
        con = duckdb.connect(database=db_path, read_only=True)
        logger.info(f"Connected to DuckDB for heavy and light carbon months: years={list(years)}")

        month_totalco2_yellow = con.execute(f"""
            SELECT
                month_of_year AS month,
                SUM(trip_co2_kgs) AS total_co2_kgs
            FROM data_transformation
            WHERE vehicle_type = 'yellow_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).fetchall()

        month_totalco2_green = con.execute(f"""
            SELECT
                month_of_year AS month,
                SUM(trip_co2_kgs) AS total_co2_kgs
            FROM data_transformation
            WHERE vehicle_type = 'green_taxi'
                AND pickup_ts >= TIMESTAMP '{start_year}-01-01'
                AND pickup_ts <  TIMESTAMP '{end_year}-01-01'
                AND trip_co2_kgs IS NOT NULL
            GROUP BY month_of_year
            ORDER BY month_of_year;
        """).fetchall()


        # plotting yellow        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), dpi=150, sharex=True, constrained_layout=True)

        month_list_yellow = []
        co2_total_list_yellow = []
        for each in month_totalco2_yellow:
            mo = each[0]
            co2 = float(each[1])
            month_list_yellow.append(mo)
            co2_total_list_yellow.append(co2)

        months = np.arange(1, 13)
        map_yellow= {}
        for month in months:
            map_yellow[int(month)] = 0.0

        for i in range(len(month_list_yellow)):
            month = int(month_list_yellow[i])
            co2 = float(co2_total_list_yellow[i])
            map_yellow[month] = co2

        month_list_yellow = list(months)
        co2_total_list_yellow = [map_yellow[m] for m in months]

        ax1.plot(month_list_yellow, co2_total_list_yellow, marker = 'o', color='#FFCE1B', label='yellow')

        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        labels = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        # ax1.xlim(1, 12)
        ax1.set_xticks(months, labels)
        # ax1.set_xlabel('Month')
        ax1.set_ylabel('Total CO2 (kg)')
        ax1.legend()


        # plotting green
        plt.figure(figsize=(10,6), dpi=150)
        
        month_list_green = []
        co2_total_list_green = []
        for each in month_totalco2_green:
            mo = each[0]
            co2 = float(each[1])
            month_list_green.append(mo)
            co2_total_list_green.append(co2)

        months = np.arange(1, 13)
        map_green = {}
        for month in months:
            map_green[int(month)] = 0.0

        for i in range(len(month_list_green)):
            month = int(month_list_green[i])
            co2 = float(co2_total_list_green[i])
            map_green[month] = co2

        month_list_green = list(months)
        co2_total_list_green = [map_green[m] for m in months]

        ax2.plot(month_list_green, co2_total_list_green, marker = 'o', color='#008000', label='green')

        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        labels = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        # ax2.xlim(1, 12)
        ax2.set_xticks(months, labels)
        ax2.set_xlabel('Month')
        ax2.set_ylabel('Total CO2 (kg)')
        ax2.legend()

        fig.suptitle('Monthly CO2 Totals by Taxi Type', fontsize=16, fontweight='bold')
        fig.savefig('./month_co2totals_yellow_green.png', dpi=150)
        plt.close(fig)

    except Exception as e:
        print(f"Unable to plot the months and co2 totals={list(years)}: {e}")
        logger.error(f"Unable to plot the months and co2 totals={list(years)}: {e}")
        return None

    finally:
        if con:
            con.close()



# Call all methods from analysis.py here
if __name__ == "__main__":
    years = range(2024, 2025)

    # SINGLE LARGEST CARBON TRIP OF THE YEARS - YELLOW THEN GREEN:
    # yellow_largest_carbon = single_largest_carbon_trip_year('yellow')
    # green_largest_carbon = single_largest_carbon_trip_year('green')
    # pretty_print_largest_carbon_trip("yellow", yellow_largest_carbon, years)
    # print("\n")
    # pretty_print_largest_carbon_trip("green", green_largest_carbon, years)

    # MIN AND MAX CARBON HOURS (AVERAGES) - YELLOW THEN GREEN:
    # results_hours = carbon_heavy_light_hour()
    # yellow_hour_min, yellow_hour_max, green_hour_min, green_hour_max = results_hours
    # print(f"yellow carbon hour min: (hour {yellow_hour_min[0]}, {yellow_hour_min[1]:.5f} kg CO2 per trip), \nyellow carbon hour max: (hour {yellow_hour_max[0]}, {yellow_hour_max[1]:.5f} kg CO2 per trip)\n")
    # print(f"green carbon hour min: (hour {green_hour_min[0]}, {green_hour_min[1]:.5f} kg CO2 per trip), \ngreen carbon hour max: (hour {green_hour_max[0]}, {green_hour_max[1]:.5f} kg CO2 per trip)\n")

    # MIN AND MAX CARBON DOW (AVERAGES) - YELLOW THEN GREEN:
    # results_DOW = carbon_heavy_light_DOW()
    # yellow_DOW_min, yellow_DOW_max, green_DOW_min, green_DOW_max = results_DOW
    # print(f"yellow carbon DOW min: ({yellow_DOW_min[0]}, {yellow_DOW_min[1]:.5f} kg CO2 per trip), \nyellow carbon DOW max: ({yellow_DOW_max[0]}, {yellow_DOW_max[1]:.5f} kg CO2 per trip)\n")
    # print(f"green carbon DOW min: ({green_DOW_min[0]}, {green_DOW_min[1]:.5f} kg CO2 per trip), \ngreen carbon DOW max: ({green_DOW_max[0]}, {green_DOW_max[1]:.5f} kg CO2 per trip)\n")

    # MIN AND MAX CARBON WEEKS (AVERAGES) - YELLOW THEN GREEN:
    # results_weeks = carbon_heavy_light_week()
    # yellow_week_min, yellow_week_max, green_week_min, green_week_max = results_weeks
    # print(f"yellow carbon week min: (week {yellow_week_min[0]},  {yellow_week_min[1]:.5f} kg CO2 per trip), \nyellow carbon week max: (week {yellow_week_max[0]},  {yellow_week_max[1]:.5f} kg CO2 per trip)\n")
    # print(f"green carbon week min: (week {green_week_min[0]},  {green_week_min[1]:.5f} kg CO2 per trip), \ngreen carbon week max: (week {green_week_max[0]},  {green_week_max[1]:.5f} kg CO2 per trip)\n")

    # MIN AND MAX CARBON MONTHS (AVERAGES) - YELLOW THEN GREEN:
    # results_months = carbon_heavy_light_month()
    # yellow_mo_min, yellow_mo_max, green_mo_min, green_mo_max = results_months
    # print(f"yellow carbon month min: (month {yellow_mo_min[0]},  {yellow_mo_min[1]:.5f} kg CO2 per trip), \nyellow carbon month max: (month {yellow_mo_max[0]},  {yellow_mo_max[1]:.5f} kg CO2 per trip)\n")
    # print(f"green carbon month min: (month {green_mo_max[0]},  {green_mo_max[1]:.5f} kg CO2 per trip), \ngreen carbon month max: (month {green_mo_max[0]},  {green_mo_max[1]:.5f} kg CO2 per trip)\n")
    
    plot_co2_month_by_co2totals()


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
