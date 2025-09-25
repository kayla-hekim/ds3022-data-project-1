import duckdb
import logging



logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)


def get_yellow_green_tables(years):
    colors = ["yellow", "green"]
    tables = []
    for year in years:
        for color in colors:
            for month in range(1,13):
                table = f"{color}_{year}_{month:02d}"
                tables.append(table)

    return tables


# remove duplicates
def remove_duplicates(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove duplicates")

        # con.execute("SET schema='tlc';")

        tables = get_yellow_green_tables(years)
            
        for each_table in tables:
            con.execute(f"""
                DROP TABLE IF EXISTS {each_table}_clean;
                CREATE TABLE {each_table}_clean AS 
                SELECT DISTINCT * FROM {each_table};
                        
                DROP TABLE {each_table};
                ALTER TABLE {each_table}_clean RENAME TO {each_table};
            """)
            logger.info(f"{each_table}: removed duplicate values")


        table_name = "vehicle_emissions"
        con.execute(f"""
            DROP TABLE IF EXISTS {table_name}_clean;
            CREATE TABLE {table_name}_clean AS 
            SELECT DISTINCT * FROM {table_name};
                    
            DROP TABLE {table_name};
            ALTER TABLE {table_name}_clean RENAME TO {table_name};
        """)
        logger.info(f"{table_name}: removed duplicate values in vehicle_emissions")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred yellow green taxi parquet or vehicle_emission loading: {e}")

    finally:
        if con:
            con.close()


# remove 0 passenger rides
def zero_passengers_removed(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with zero passengers")

        tables = get_yellow_green_tables(years)

        for each_table in tables:
            con.execute(f"""
                DELETE FROM {each_table} 
                WHERE passenger_count <= 0;
            """)
            logger.info(f"{each_table}: removed zero/negative passengers ride observations")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with zero passengers: {e}")

    finally:
        if con:
            con.close()


# remove 0 mile rides
def zero_miles_removed(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with zero miles")

        tables = get_yellow_green_tables(years)

        for each_table in tables:
            con.execute(f"""
                DELETE FROM {each_table} 
                WHERE trip_distance <= 0.0;
            """)
            logger.info(f"{each_table}: removed zero/negative mile ride observations")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with zero miles: {e}")

    finally:
        if con:
            con.close()


# remove more than 100 miles rides
def more_100mi_removed(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with more than 100 miles")

        tables = get_yellow_green_tables(years)

        for each_table in tables:
            con.execute(f"""
                DELETE FROM {each_table} 
                WHERE trip_distance > 100.0;
            """)
            logger.info(f"{each_table}: removed more than 100 miles ride observations")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with more than 100 miles: {e}")

    finally:
        if con:
            con.close()


# helper method for 24 hr rides
def get_datetime_cols(con, table):
    # Grab column names for the table
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    pickup = None
    dropoff = None
    if "tpep_pickup_datetime" in cols and "tpep_dropoff_datetime" in cols:
        pickup, dropoff = "tpep_pickup_datetime", "tpep_dropoff_datetime"
    elif "lpep_pickup_datetime" in cols and "lpep_dropoff_datetime" in cols:
        pickup, dropoff = "lpep_pickup_datetime", "lpep_dropoff_datetime"
    return pickup, dropoff

# remove more than 24 hour rides
def more_24hr_removed(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with more than 24 hours")

        tables = get_yellow_green_tables(years)

        # make time column in each table to use later
        for each_table in tables:
            pickup_col, dropoff_col = get_datetime_cols(con, each_table)
            if not pickup_col or not dropoff_col:
                logger.warning(f"{each_table}: no recognized pickup/dropoff columns; skipping")
                continue

            # con.execute(f"""
            #     ALTER TABLE {each_table}
            #     ADD COLUMN time_hours DOUBLE;

            #     UPDATE {each_table}
            #     SET time_hours = EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) / 3600.0
            #     WHERE {pickup_col} IS NOT NULL AND {dropoff_col} IS NOT NULL;
            # """)
            # logger.info(f"{each_table}: calculated time_hours and ready to remove greater than 24 hours rides")

            # con.execute(f"""
            #     DELETE FROM {each_table} 
            #     WHERE time_hours > 24.0 OR time_hours < 0;
            # """)

            # don't need to create table
            con.execute(f"""
                DELETE FROM {each_table}
                WHERE {pickup_col} IS NOT NULL
                  AND {dropoff_col} IS NOT NULL
                  AND (
                        ({dropoff_col} - {pickup_col}) > INTERVAL 24 HOUR
                     OR ({dropoff_col} - {pickup_col}) <= INTERVAL 0 SECOND
                  );
            """)
            logger.info(f"{each_table}: removed more than 24 hours ride observations")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with more than 24 hours: {e}")

    finally:
        if con:
            con.close()


# testing all methods above
def tests(years=range(2024, 2025)):
    con = None
    failures = []
    try:
        con = duckdb.connect(database='cleancopy.duckdb', read_only=True)
        logger.info("Running tests for above methods...")

        tables = get_yellow_green_tables(years)

        for t in tables:
            # test to make sure table does exist
            try:
                con.execute(f"SELECT 1 FROM {t} LIMIT 1")
            except Exception as e:
                failures.append(f"[{t}] missing or unreadable: {e}")
                continue

            # test to make sure duplicates were removed
            total_rows = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            distinct_rows = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {t})").fetchone()[0]
            if total_rows != distinct_rows:
                failures.append(f"[{t}] duplicates remain: total={total_rows}, distinct={distinct_rows}")

            # test to make sure there are more than 0 passengers per ride
            bad_pass = con.execute(
                f"SELECT COUNT(*) FROM {t} WHERE passenger_count <= 0"
            ).fetchone()[0]
            if bad_pass > 0:
                failures.append(f"[{t}] passenger_count <= 0 rows: {bad_pass}")

            # test to make sure trip distances are more than 0 AND not greater than 100
            bad_dist = con.execute(
                f"SELECT COUNT(*) FROM {t} WHERE trip_distance <= 0 OR trip_distance > 100"
            ).fetchone()[0]
            if bad_dist > 0:
                failures.append(f"[{t}] out-of-bounds trip_distance rows: {bad_dist}")

            # test to make sure hours calculated from start to drop off aren't 0 or less nor over 24 hrs
            cols = [r[1] for r in con.execute(f"PRAGMA table_info('{t}')").fetchall()]
            if "tpep_pickup_datetime" in cols and "tpep_dropoff_datetime" in cols:
                pickup, dropoff = "tpep_pickup_datetime", "tpep_dropoff_datetime"
            elif "lpep_pickup_datetime" in cols and "lpep_dropoff_datetime" in cols:
                pickup, dropoff = "lpep_pickup_datetime", "lpep_dropoff_datetime"
            else:
                pickup, dropoff = None, None
            if pickup and dropoff:
                bad_time = con.execute(f"""
                    WITH u AS (
                        SELECT EXTRACT(EPOCH FROM ({dropoff} - {pickup})) / 3600.0 AS hrs
                        FROM {t}
                        WHERE {pickup} IS NOT NULL AND {dropoff} IS NOT NULL
                    )
                    SELECT COUNT(*) FROM u WHERE hrs <= 0 OR hrs > 24
                """).fetchone()[0]
                if bad_time > 0:
                    failures.append(f"[{t}] out-of-bounds duration rows: {bad_time}")

            logger.info(f"[{t}] tested OK")

        # testing vehicle_emissions table - smaller than other yellow green ones
        table = "vehicle_emissions"
        try:
            con.execute(f"SELECT 1 FROM {table} LIMIT 1")
            total_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            distinct_rows = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})").fetchone()[0]
            if total_rows != distinct_rows:
                failures.append(f"[{table}] duplicates remain: total={total_rows}, distinct={distinct_rows}")

            neg_co2 = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE co2_grams_per_mile < 0"
            ).fetchone()[0]
            if neg_co2 > 0:
                failures.append(f"[{table}] negative co2_grams_per_mile rows: {neg_co2}")

            unreasonable_years = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE vehicle_year_avg IS NULL OR vehicle_year_avg < 1980 OR vehicle_year_avg > 2035
            """).fetchone()[0]
            if unreasonable_years > 0:
                failures.append(f"[{table}] out-of-range/NULL vehicle_year_avg rows: {unreasonable_years}")

            logger.info(f"[{table}] tested OK")
        except Exception as e:
            failures.append(f"[{table}] missing or unreadable: {e}")

        # Final results print of all tests pass, or some fail :(
        if failures:
            print(f"tests have FAILED for {len(failures)} test cases in clean")
            for f in failures:
                print(" -", f)
            logger.error(f"tests have FAILED for all methods in clean with {len(failures)} failed tests ")
        else:
            print("tests have passed for all methods in clean")
            logger.info("tests have passed for all methods in clean")

    finally:
        if con:
            con.close()



# Call all methods from load.py here
if __name__ == "__main__":
    # remove duplicates
    remove_duplicates()

    # # remove trips with 0 passengers
    zero_passengers_removed()

    # remove trips 0 miles in length
    zero_miles_removed()

    # remove trips greater than 100 miles in length
    more_100mi_removed()

    # remove trips greater than 24 hours in length
    more_24hr_removed()

    # include tests - all methods above !
    tests()