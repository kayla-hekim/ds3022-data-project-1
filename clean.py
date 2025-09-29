import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)


# get yellow green tables for later
def get_yellow_green_tables(years=(2024, 2025)):    
    tables = []
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False) # testing

    try:
        for year in years:
            table = f"yellow_{year}"
            table = remove_duplicates_yellow_green(con, table)
            if not table:  
                continue
            table = drop_columns_yellow(con, table)
            if table:
                tables.append(table)

        for year in years:
            table = f"green_{year}"
            table = remove_duplicates_yellow_green(con, table)
            if not table:  
                continue
            table = drop_columns_green(con, table)
            if table:
                tables.append(table)

        return tables
    
    finally:
        con.close()
    


# helper method - drop yellow columns
def drop_columns_yellow(con, table):
    temp = f"{table}_clean"
    try:
        # transaction begin/commit documentation: https://duckdb.org/docs/stable/sql/statements/transactions.html
        con.execute("BEGIN TRANSACTION;")
        con.execute(f"""
            CREATE OR REPLACE TABLE {temp} AS
            SELECT
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance
            FROM {table};
        """)
        con.execute(f"DROP TABLE {table};")
        con.execute(f"ALTER TABLE {temp} RENAME TO {table};")
        con.execute("COMMIT;")
        return table
    
    except Exception as e:
        logger.error(f"Issue dropping yellow columns in {table}: {e}")

        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass

        try:
            con.execute(f"DROP TABLE IF EXISTS {temp};")
        except Exception:
            pass

        return None

# helper method - drop green columns
def drop_columns_green(con, table):
    temp = f"{table}_clean"
    try:
        con.execute("BEGIN TRANSACTION;")
        con.execute(f"""
            CREATE OR REPLACE TABLE {temp} AS
            SELECT
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                passenger_count,
                trip_distance
            FROM {table};
        """)
        con.execute(f"DROP TABLE {table};")
        con.execute(f"ALTER TABLE {temp} RENAME TO {table};")
        con.execute("COMMIT;")
        return table
    
    except Exception as e:
        logger.error(f"Issue dropping green columns in {table}: {e}")

        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass

        try:
            con.execute(f"DROP TABLE IF EXISTS {temp};")
        except Exception:
            pass
        
        return None
    

def remove_duplicates_yellow_green(con, table):
    temp = f"{table}_rmduplicates"
    try:
        con.execute(f"""BEGIN TRANSACTION;""")
        con.execute(f"DROP TABLE IF EXISTS {temp};")
        con.execute(f"""
            CREATE OR REPLACE TABLE {temp} AS
            SELECT DISTINCT * FROM {table};
        """)
        con.execute(f"DROP TABLE {table};")
        con.execute(f"ALTER TABLE {temp} RENAME TO {table};")
        con.execute(f"""COMMIT;""")
        return table
    
    except Exception as e:
        print(f"wasn't able to remove duplicates for {table}")
        logger.error(f"wasn't able to remove duplicates for {table}")

        try: 
            con.execute("ROLLBACK;")
        except Exception as e: 
            pass
        try: 
            con.execute(f"DROP TABLE IF EXISTS {temp};")
        except Exception as e: 
            pass
        
        return None


# remove duplicates
def remove_duplicates_vehicle_emissions():
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)

        logger.info("Connected to DuckDB, ready to remove duplicates for vehicle_emissions")

        exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'vehicle_emissions'
        """).fetchone()[0]
        if not exists:
            logger.warning("vehicle_emissions table not found; skipping dedupe.")
            return False

        table_name = "vehicle_emissions"
        temp = f"{table_name}_rmduplicates"
        con.execute(f"""BEGIN TRANSACTION;""")
        con.execute(f"""
            CREATE OR REPLACE TABLE {temp} AS
            SELECT DISTINCT * FROM {table_name};
        """)
        con.execute(f"DROP TABLE {table_name};")
        con.execute(f"ALTER TABLE {temp} RENAME TO {table_name};")
        con.execute(f"""COMMIT;""")
        logger.info(f"{table_name}: removed duplicate values in vehicle_emissions")
        return table_name

    except Exception as e:
        print(f"An error occurred for removing duplicates from vehicle_emisisons: {e}")
        logger.error(f"An error occurred for removing duplicates from vehicle_emisisons: {e}")

        try: 
            con.execute("ROLLBACK;")
        except Exception: 
            pass
        try: 
            con.execute(f"DROP TABLE IF EXISTS {temp};")
        except Exception: 
            pass

        return None

    finally:
        if con:
            con.close()


# remove 0 passenger rides
def zero_passengers_removed(tables):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with zero passengers")

        # tables = get_yellow_green_tables(years)

        for each_table in tables:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info('{each_table}')").fetchall()]
            if "passenger_count" not in cols:
                logger.warning(f"{each_table}: no passenger_count column; skipping")
                continue

            try:
                con.execute(f"""BEGIN TRANSACTION;""")
                con.execute(f"""
                    DELETE FROM {each_table} 
                    WHERE COALESCE(passenger_count, 0) <= 0
                """)
                con.execute("COMMIT;")
                logger.info(f"{each_table}: removed zero/negative passengers ride observations")

            except Exception as e:
                try: 
                    con.execute("ROLLBACK;")
                except Exception: 
                    pass
                logger.error(f"{each_table}: failed to remove zero/NULL passengers: {e}")

    except Exception as e:
        print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with zero passengers: {e}")

    finally:
        if con:
            con.close()


# remove 0 mile rides
def zero_miles_removed(tables):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with zero miles")

        # tables = get_yellow_green_tables(years)

        for each_table in tables:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info('{each_table}')").fetchall()]
            if "trip_distance" not in cols:
                logger.warning(f"{each_table}: no trip_distance column; skipping")
                continue

            try:
                con.execute(f"""BEGIN TRANSACTION;""")
                con.execute(f"""
                    DELETE FROM {each_table} 
                    WHERE COALESCE(CAST(trip_distance AS DOUBLE), 0) <= 0.0;
                """)
                con.execute("COMMIT;")
                logger.info(f"{each_table}: removed zero/negative mile ride observations")
            
            except Exception as e:
                try: 
                    con.execute("ROLLBACK;")
                except Exception: 
                    pass
                logger.error(f"{each_table}: failed to remove trips with 0 miles: {e}")

    except Exception as e:
        print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with zero miles: {e}")

    finally:
        if con:
            con.close()


# remove more than 100 miles rides
def more_100mi_removed(tables):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with more than 100 miles")

        # tables = get_yellow_green_tables(years)

        for each_table in tables:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info('{each_table}')").fetchall()]
            if "trip_distance" not in cols:
                logger.warning(f"{each_table}: no trip_distance column; skipping")
                continue
            
            try:
                con.execute(f"""BEGIN TRANSACTION;""")
                con.execute(f"""
                    DELETE FROM {each_table} 
                    WHERE COALESCE(CAST(trip_distance AS DOUBLE), 0) > 100.0;
                """)
                con.execute("COMMIT;")
                logger.info(f"{each_table}: removed more than 100 miles ride observations")
            except Exception as e:
                try: 
                    con.execute("ROLLBACK;")
                except Exception: 
                    pass
                logger.error(f"{each_table}: failed to remove trips with more than 100 miles: {e}")

    except Exception as e:
        print(f"An error occurred for yellow green taxi parquet loading: {e}")
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
def more_24hr_removed(tables):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with more than 24 hours")

        # tables = get_yellow_green_tables(years)

        # make time column in each table to use later
        for each_table in tables:
            pickup_col, dropoff_col = get_datetime_cols(con, each_table)
            if not pickup_col or not dropoff_col:
                logger.warning(f"{each_table}: no recognized pickup/dropoff columns; skipping")
                continue
            
            try:
                con.execute(f"""BEGIN TRANSACTION;""")
                con.execute(f"""
                    DELETE FROM {each_table}
                    WHERE {pickup_col} IS NOT NULL AND {dropoff_col} IS NOT NULL
                        AND (
                            date_diff('second', {pickup_col}, {dropoff_col}) <= 0
                            OR date_diff('second', {pickup_col}, {dropoff_col}) > {24 * 3600}
                        );
                """)
                con.execute("COMMIT;")
                logger.info(f"{each_table}: removed more than 24 hours ride observations")
            except Exception as e:
                try: 
                    con.execute("ROLLBACK;")
                except Exception: 
                    pass
                logger.error(f"{each_table}: failed to remove trips that are more than 24 hours: {e}")

    except Exception as e:
        print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with more than 24 hours: {e}")

    finally:
        if con:
            con.close()


# testing all methods above
def tests(tables):
    con = None
    failures = []
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        # con = duckdb.connect(database='emissionscopy.duckdb', read_only=False)
        logger.info("Running tests for above methods...")

        # tables = get_yellow_green_tables(years)

        for table in tables:
            # 1. test to make sure table does exist
            try:
                con.execute(f"SELECT 1 FROM {table} LIMIT 1")
            except Exception as e:
                failures.append(f"[{table}] missing or unreadable: {e}")
                logger.warning(f"{table} is missing or unreadable: {e}")
                continue

            # 2. test to make sure duplicates were removed
            total_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            distinct_rows = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})").fetchone()[0]
            if total_rows != distinct_rows:
                failures.append(f"[{table}] duplicates remain: total={total_rows}, distinct={distinct_rows}")
                logger.warning(f"issue arose in {table}, total rows isn't equal to non-duplicated rows !")
            else:
                logger.info(f"{table}'s num total rows == num non-duplicated rows !")


            # 3. test to make sure there are more than 0 passengers per ride
            bad_passenger = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE passenger_count IS NULL OR passenger_count <= 0"
            ).fetchone()[0]
            if bad_passenger > 0:
                failures.append(f"[{table}] passenger_count <= 0 rows: {bad_passenger}")
                logger.warning(f"issue arose in {table}, there are at least 1 ride(s) with 0 passengers !")
            else:
                logger.info(f"all rides in {table} have at least 1 passenger !")


            # 4. test to make sure trip distances are more than 0 AND not greater than 100
            bad_distance = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE trip_distance IS NULL OR trip_distance <= 0 OR trip_distance > 100"
            ).fetchone()[0]
            if bad_distance > 0:
                failures.append(f"[{table}] out-of-bounds trip_distance rows: {bad_distance}")
                logger.warning(f"issue arose in {table}, there exists at least 1 trip(s) where the ride is either 0 or less, or more than 100 miles !")
            else:
                logger.info(f"all rides in {table} are between 1 inclusive and 100 inclusive")


            # 5. test to make sure hours calculated from start to drop off aren't 0 or less nor over 24 hrs
            pickup, dropoff = None, None
            cols = [row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            if "tpep_pickup_datetime" in cols and "tpep_dropoff_datetime" in cols:
                pickup, dropoff = "tpep_pickup_datetime", "tpep_dropoff_datetime"
            elif "lpep_pickup_datetime" in cols and "lpep_dropoff_datetime" in cols:
                pickup, dropoff = "lpep_pickup_datetime", "lpep_dropoff_datetime"

            if pickup and dropoff:
                dup_groups = con.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pickup}, {dropoff}, passenger_count, trip_distance, COUNT(*) c
                        FROM {table}
                        GROUP BY {pickup}, {dropoff}, passenger_count, trip_distance
                        HAVING COUNT(*) > 1
                    )
                """).fetchone()[0]
                if dup_groups > 0:
                    failures.append(f"[{table}] duplicate groups by key: {dup_groups}")
                    logger.warning(f"issue arose in {table}: there exists at least 1 pair of duplicated items in table !")
                else:
                    logger.info(f"{table} has no duplicated values")

            if pickup and dropoff:
                null_ts = con.execute(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE {pickup} IS NULL OR {dropoff} IS NULL
                """).fetchone()[0]
                if null_ts:
                    logger.warning(f"[{table}] rows with NULL {pickup}/{dropoff}: {null_ts}")

                bad_time = con.execute(f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE {pickup} IS NOT NULL
                        AND {dropoff} IS NOT NULL
                        AND (
                            date_diff('second', {pickup}, {dropoff}) <= 0
                            OR date_diff('second', {pickup}, {dropoff}) > {24*3600}
                    )
                """).fetchone()[0]

                if bad_time > 0:
                    failures.append(f"[{table}] out-of-bounds duration rows: {bad_time}")
                    logger.warning(f"issue arose in {table}, there exists at least 1 trip(s) where the ride is either 0 or less hours, or more than 24 hrs !")
                else:
                    logger.info(f"all trips in {table} are either between 1 inclusive to 24 hours inclusive")

        # 6. testing vehicle_emissions table - smaller than other yellow green ones
        table = "vehicle_emissions"
        try:
            con.execute(f"SELECT 1 FROM {table} LIMIT 1")
            total_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            distinct_rows = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})").fetchone()[0]
            if total_rows != distinct_rows:
                failures.append(f"[{table}] duplicates remain: total={total_rows}, distinct={distinct_rows}")
                logger.warning(f"issue arose: {table} has duplicates !")
            else:
                logger.info(f"{table} has no duplicates")

            neg_co2 = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE co2_grams_per_mile < 0"
            ).fetchone()[0]
            if neg_co2 > 0:
                failures.append(f"[{table}] negative co2_grams_per_mile rows: {neg_co2}")
                logger.warning(f"issue arose: {table} has negative co2_grams_per_mile !")
            else: 
                logger.info(f"{table} has no negative co2_grams_per_mile values")

            unreasonable_years = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE vehicle_year_avg IS NULL OR vehicle_year_avg < 1980 OR vehicle_year_avg > 2035
            """).fetchone()[0]
            if unreasonable_years > 0:
                failures.append(f"[{table}] out-of-range/NULL vehicle_year_avg rows: {unreasonable_years}")
                logger.warning(f"{table} has years less than 1980 or greater than 2035 !")
            else:
                logger.info(f"{table} has no years outside the range of 1980 to 2035")

            logger.info(f"[{table}] tested OK")

        except Exception as e:
            failures.append(f"[{table}] missing or unreadable: {e}")
            logger.warning(f"{table} is missing or unreadable")

        # Final results print of all tests pass, or some fail :(
        if failures:
            print(f"tests have FAILED for {len(failures)} test cases in clean")
            for f in failures:
                print(" -", f)
            logger.error(f"tests have FAILED for all methods in clean with {len(failures)} failed tests ")
        else:
            print("tests have passed for all methods in clean")
            logger.info("tests have passed for all methods in clean - nothing in list 'failures'")

    finally:
        if con:
            con.close()



# Call all methods from load.py here
if __name__ == "__main__":
    # get tables for later methods:
    # years = range(2015, 2025) 
    years = range(2023, 2025) # testing
    tables = get_yellow_green_tables(years)

    # remove duplicates vehicle_emissions (yellow green is in get_yellow_green_tables)
    remove_duplicates_vehicle_emissions()

    # remove trips with 0 passengers
    zero_passengers_removed(tables)

    # remove trips 0 miles in length
    zero_miles_removed(tables)

    # remove trips greater than 100 miles in length
    more_100mi_removed(tables)

    # remove trips greater than 24 hours in length
    more_24hr_removed(tables)

    # include tests - all methods above !
    tests(tables)