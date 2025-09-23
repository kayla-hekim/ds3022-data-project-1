import duckdb
import os
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)



# Loading yellow and green parquet files into emissions db
def load_parquet_files():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance for yellow green taxi parquets")

        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("PRAGMA enable_object_cache=true;")
        con.execute("CREATE SCHEMA IF NOT EXISTS tlc;")



        # uploading and populating yellow 2024 tables from parquet
        year = "2024"
        color = "yellow"
        for month in range(1,13):
            input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2024-{month:02d}.parquet"
            logger.info(f"working on {input_file} now...")

            table_name = f"{color}_{year}_{month:02d}"
            
            con.execute(f"""
                -- SQL goes here
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name}
                    AS
                SELECT * FROM read_parquet('{input_file}', union_by_name=true);
            """)
            logger.info(f"Dropped if exists and created table {table_name} in emissions db")

        

        # uploading and populating green 2024 tables from parquet
        color = "green"
        for month in range(1,13):
            input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2024-{month:02d}.parquet"
            logger.info(f"working on {input_file} now...")

            table_name = f"{color}_{year}_{month:02d}"
            
            con.execute(f"""
                -- SQL goes here
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name}
                    AS
                SELECT * FROM read_parquet('{input_file}', union_by_name=true);
            """)
            logger.info(f"Dropped if exists and created table {table_name} in emissions db")


        
        # in the future: aggregate all yellow green tables into 1 big one, but not for now
        # con.execute("""
        #     DROP TABLE IF EXISTS trips_2024;
        #     CREATE TABLE trips_2024 AS
        #     SELECT *, 'yellow' AS taxi_color, '2024-' || lpad(CAST(month AS VARCHAR), 2, '0') AS yyyymm
        #     FROM (
        #         -- UNION all yellow months
        #         SELECT *, 1 AS month FROM yellow_2024_01
        #         UNION ALL SELECT *, 2 FROM yellow_2024_02
        #         ...
        #         UNION ALL SELECT *, 12 FROM yellow_2024_12
        #     )
        #     UNION ALL
        #     SELECT *, 'green' AS taxi_color, ...
        #     -- same for green tables
        # """)

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred yellow green taxi parquet loading: {e}")

    finally:
        if con:
            con.close()



# Loading vehicle_emissions.csv into emissions db
def load_vehicle_emissions_csv(file_name):
    con = None
    table_name = "vehicle_emissions"

    try:
         # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance for vehicle emissions csv table")

        con.execute("PRAGMA enable_object_cache=true;")
        con.execute("CREATE SCHEMA IF NOT EXISTS tlc;")


        con.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
                    AS
            SELECT * FROM read_csv_auto('{file_name}', HEADER=TRUE);
        """)
        logger.info(f"Loaded {file_name} into {table_name} table into emissions db")
        cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"{table_name} has {cnt} rows")

    except Exception as e:
        # print(f"An error occurred for vehicle emissions loading: {e}")
        logger.error(f"An error occurred for vehicle emissions loading: {e}")
    
    finally:
        if con:
            con.close()



# performing basic summarizations in sql through duckdb connection 
def basic_data_summarizations():
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Performing basic summarizations on green, yellow, and vehicle emissions tables in emissions db")

        # summing those in yellow and green tables
        # yellow sum
        tables = [f"yellow_2024_{m:02d}" for m in range(1, 13)]
        union_sql_yellow = " UNION ALL ".join([f"SELECT COUNT(*) AS count FROM {t}" for t in tables])
        query = f"SELECT SUM(count) FROM ({union_sql_yellow})"
        total_yellow = con.execute(query).fetchone()[0]
        logger.info("summing count from yellow tables")

         # green sum
        tables = [f"green_2024_{m:02d}" for m in range(1, 13)]
        union_sql_green = " UNION ALL ".join([f"SELECT COUNT(*) AS count FROM {t}" for t in tables])
        query = f"SELECT SUM(count) FROM ({union_sql_green})"
        total_green = con.execute(query).fetchone()[0]
        logger.info("summing count from green tables")

        # logging sums and printing
        print(f"Total yellow trips 2024: {total_yellow:,}")
        print(f"Total green trips 2024: {total_green:,}")
        logger.info(f"Total yellow trips 2024: {total_yellow:,}")
        logger.info(f"Total green trips 2024: {total_green:,}")


        # Average trip distance (sample from one month each color)
        avg_yellow_dist = con.execute("SELECT AVG(trip_distance) FROM yellow_2024_01").fetchone()[0]
        avg_green_dist = con.execute("SELECT AVG(trip_distance) FROM green_2024_01").fetchone()[0]
        print(f"Average trip distance (yellow Jan 2024): {avg_yellow_dist:.2f} miles")
        print(f"Average trip distance (green Jan 2024): {avg_green_dist:.2f} miles")
        logger.info(f"Average trip distance (yellow Jan 2024): {avg_yellow_dist:.2f} miles")
        logger.info(f"Average trip distance (green Jan 2024): {avg_green_dist:.2f} miles")


        # Vehicle emissions table summary
        cnt_emissions = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        avg_emission = con.execute("SELECT AVG(co2_grams_per_mile) FROM vehicle_emissions").fetchone()[0]
        print(f"vehicle_emissions rows: {cnt_emissions}, avg CO2 g/mi: {avg_emission:.2f}")
        logger.info(f"vehicle_emissions rows: {cnt_emissions}, avg CO2 g/mi: {avg_emission:.2f}")

    except Exception as e:
        print(f"Error in summarization: {e}")
        logger.error(f"Error in summarization: {e}")
    finally:
        if con:
            con.close()



# Call all methods from load.py here
if __name__ == "__main__":
    # populate and create tables yellow green 
    load_parquet_files()
    vehicle_emissions_csv = './data/vehicle_emissions.csv'
    load_vehicle_emissions_csv(vehicle_emissions_csv)
    # summarizations
    basic_data_summarizations()
