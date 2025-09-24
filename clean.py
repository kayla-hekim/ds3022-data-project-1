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
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
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


        # table_name = "vehicle_emissions"
        # con.execute(f"""
        #     DROP TABLE IF EXISTS {table_name}_clean;
        #     CREATE TABLE {table_name}_clean AS 
        #     SELECT DISTINCT * FROM {table_name};
                    
        #     DROP TABLE {table_name};
        #     ALTER TABLE {table_name}_clean RENAME TO {table_name};
        # """)
        # logger.info(f"{table_name}: removed duplicate values in vehicle_emissions")

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
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
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
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
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
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
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


# remove more than 24 hour rides
def more_24hr_removed(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove rides with more than 24 hours")

        tables = get_yellow_green_tables(years)

        # make time column in each table to use later
        for each_table in tables:
            con.execute(f"""
                ALTER TABLE {each_table}
                ADD COLUMN time_hours DOUBLE;

                UPDATE {each_table}
                SET time_hours = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0;
            """)
            logger.info(f"{each_table}: calculated time_hours and ready to remove greater than 24 hours rides")

            con.execute(f"""
                DELETE FROM {each_table} 
                WHERE time_hours > 24.0;
            """)
            logger.info(f"{each_table}: removed more than 24 hours ride observations")

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred while removing rides with more than 24 hours: {e}")

    finally:
        if con:
            con.close()


def tests():
    con = None


# Call all methods from load.py here
if __name__ == "__main__":
    # remove duplicates
    remove_duplicates()

    # remove trips with 0 passengers
    zero_passengers_removed()

    # remove trips 0 miles in length
    zero_miles_removed()

    # remove trips greater than 100 miles in length
    more_100mi_removed()

    # remove trips greater than 24 hours in length
    more_24hr_removed()

    # include tests
    tests()