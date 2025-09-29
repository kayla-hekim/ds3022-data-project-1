import duckdb
import os
import logging
import time


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)



# Loading yellow and green parquet files into emissions db
def load_parquet_files(years=range(2024, 2025)):
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance for yellow green taxi parquets")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("PRAGMA enable_object_cache=true;")

        # con.execute("CREATE SCHEMA IF NOT EXISTS tlc;")

        # year = "2024"
        for year in years:
            # uploading and populating yellow 2024 table from parquet
            color = "yellow"
            table_name = f"{color}_{year}"
            con.execute(f"DROP TABLE IF EXISTS {table_name};")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-01.parquet', union_by_name=true);")
            logger.info(f"Dropped if exists and created table {table_name} for month 01 in emissions db")

            for month in range(2,13):
                input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
                logger.info(f"working on {input_file} now...")
                
                try:
                    con.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM read_parquet('{input_file}', union_by_name=true);
                    """)

                    logger.info(f"Dropped if exists and created table {table_name} for month {month:02d} in emissions db")
                    time.sleep(45)
                except Exception as e:
                    logger.warning(f"Skipping {input_file} due to error: {e}")
                    continue

            # table_count =  con.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()[0]
            # print(f"Count for table {table_name}: {table_count}")
            # logger.info(f"Count for table {table_name}: {table_count}")
            

            # uploading and populating green 2024 table from parquet
            color = "green"
            table_name = f"{color}_{year}"
            con.execute(f"DROP TABLE IF EXISTS {table_name};")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-01.parquet', union_by_name=true);")
            logger.info(f"Dropped if exists and created table {table_name} for month 01 in emissions db")


            for month in range(2,13):
                input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
                logger.info(f"working on {input_file} now...")
                
                try:
                    con.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM read_parquet('{input_file}', union_by_name=true);
                    """)
                    logger.info(f"Dropped if exists and created table {table_name} for month {month:02d} in emissions db")
                    time.sleep(45)
                except Exception as e:
                    logger.warning(f"Skipping {input_file} due to error: {e}")
                    continue

            # table_count =  con.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()[0]
            # print(f"Count for table {table_name}: {table_count}")
            # logger.info(f"Count for table {table_name}: {table_count}")

        # con.execute("VACUUM;") # had issues with disc space - research said this would help?
        # logger.info("VACUUM completed")

    except Exception as e:
        print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.warning(f"An error occurred yellow green taxi parquet loading: {e}")

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

        # con.execute("CREATE SCHEMA IF NOT EXISTS tlc;")
        # con.execute("SET schema='tlc';")        

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
        print(f"An error occurred for vehicle emissions loading: {e}")
        logger.warning(f"An error occurred for vehicle emissions loading: {e}")
    
    finally:
        if con:
            con.close()



# performing basic summarizations in sql through duckdb connection FOR 2024 FOR NOW
def basic_data_summarizations(years=range(2024, 2025)):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Performing basic summarizations on green, yellow, and vehicle emissions tables in emissions db")


    # SUMMING ROW AMOUNTS FROM YELLOW THEN GREEN TABLES
        # check counts of tables yellow green
        total_yellow = 0
        total_green = 0
        for year in years:
            table = f"yellow_{year}"
            try:
                year_num = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception as e:
                year_num = 0
                logger.warning(f"{table} missing or unreadable in yellow sum: {e}")
            total_yellow += year_num
            print(f"{table} rows: {year_num}")
            logger.info(f"{table} rows: {year_num}")

        for year in years:
            table = f"green_{year}"
            try:
                year_num = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception as e:
                year_num = 0
                logger.warning(f"{table} missing or unreadable in green sum: {e}")
            total_green += year_num
            print(f"{table} rows: {year_num}")
            logger.info(f"{table} rows: {year_num}")

        print("\n")

        print(f"Total yellow trips (all years): {total_yellow}")
        print(f"Total green trips (all years): {total_green}")
        logger.info(f"Total yellow trips (all years): {total_yellow}")
        logger.info(f"Total green trips (all years): {total_green}")


        print("---------------------------------")


    # AVERAGE TRIP DISTANCE PER TABLE, THEN ALL TOGETHER
        sum_distance_yellow_all = 0.0 # by months out of all years together
        sum_rows_yellow_all = 0
        averages_per_year_yellow = {}
        
        # avg by yellow
        for year in years:
            table = f"yellow_{year}"
            try:
                sum_distance, num_rows = con.execute(f"""
                    SELECT CAST(COALESCE(SUM(trip_distance), 0) AS DOUBLE), COUNT(trip_distance)
                    FROM {table}
                """).fetchone()
            except Exception as e:
                sum_distance, num_rows = 0.0, 0
                logger.warning(f"{table} missing or unreadable in yellow avg dist: {e}")
            
            if num_rows:
                year_avg = (sum_distance / num_rows)
            else:
                year_avg = 0.0
            averages_per_year_yellow[year] = year_avg
            sum_distance_yellow_all += sum_distance
            sum_rows_yellow_all += num_rows
            logger.info(f"obtained average for {table}")

        overall_yellow_avg = 0.0
        if sum_rows_yellow_all:
            overall_yellow_avg = (sum_distance_yellow_all / sum_rows_yellow_all) 
        else:
            overall_yellow_avg = 0.0


        sum_distance_green_all = 0.0
        sum_rows_green_all = 0
        averages_per_year_green = {}

        for year in years:
            table = f"green_{year}"
            try:
                sum_distance, num_rows = con.execute(f"""
                    SELECT CAST(COALESCE(SUM(trip_distance), 0) AS DOUBLE), COUNT(trip_distance)
                    FROM {table}
                """).fetchone()
            except Exception as e:
                sum_distance, num_rows = 0.0, 0
                logger.warning(f"{table} missing or unreadable in green avg dist: {e}")
            
            if num_rows:
                year_avg = (sum_distance / num_rows)
            else:
                year_avg = 0.0
            averages_per_year_green[year] = year_avg
            sum_distance_green_all += sum_distance
            sum_rows_green_all += num_rows
            logger.info(f"obtained average for {table}")

        overall_green_avg = 0.0
        if sum_rows_green_all:
            overall_green_avg = (sum_distance_green_all / sum_rows_green_all)
        else:
            overall_green_avg = 0.0
        

        print(f"total average for all yellow tables: {overall_yellow_avg}")
        print(f"total average for all green tables: {overall_green_avg}")
        logger.info(f"total average for all yellow tables: {overall_yellow_avg}")
        logger.info(f"total average for all green tables: {overall_green_avg}")


        print("\n")

        for year, avg in averages_per_year_yellow.items():
            print(f"{year} yellow averages: {avg}")
        
        print("\n")
        
        for year, avg in averages_per_year_green.items():
            print(f"{year} green averages: {avg}")


        print("---------------------------------")


    # VEHICLE EMISSIONS TABLE SUMMARIES
        cnt_emissions = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        avg_emission = con.execute("SELECT AVG(co2_grams_per_mile) FROM vehicle_emissions").fetchone()[0]
        min_emission = con.execute("SELECT MIN(co2_grams_per_mile) FROM vehicle_emissions").fetchone()[0]
        max_emission = con.execute("SELECT MAX(co2_grams_per_mile) FROM vehicle_emissions").fetchone()[0]
        median_emission = con.execute("SELECT median(co2_grams_per_mile) FROM vehicle_emissions").fetchone()[0]
        vehicle_year_avg = con.execute("SELECT AVG(vehicle_year_avg) FROM vehicle_emissions").fetchone()[0]

        print(f"vehicle_emissions rows: {cnt_emissions}")
        print(f"vehicle_emissions average CO2 g/mi: {avg_emission}")
        print(f"vehicle_emissions min CO2 g/mi: {min_emission}")
        print(f"vehicle_emissions max CO2 g/mi: {max_emission}")
        print(f"vehicle_emissions median CO2 g/mi: {median_emission}")
        print(f"vehicle year average average: {vehicle_year_avg}")
        logger.info(f"vehicle_emissions rows: {cnt_emissions}")
        logger.info(f"vehicle_emissions average CO2 g/mi: {avg_emission}")
        logger.info(f"vehicle_emissions min CO2 g/mi: {min_emission}")
        logger.info(f"vehicle_emissions max CO2 g/mi: {max_emission}")
        logger.info(f"vehicle_emissions median CO2 g/mi: {median_emission}")
        logger.info(f"vehicle year average average: {vehicle_year_avg}")


    except Exception as e:
        print(f"Error in summarization: {e}")
        logger.warning(f"Error in summarization: {e}")
    finally:
        if con:
            con.close()


# Call all methods from load.py here
if __name__ == "__main__":
    years = range(2015, 2025)
    # years = range(2023, 2025) # testing
    # populate and create tables yellow green 
    load_parquet_files(years)
    vehicle_emissions_csv = './data/vehicle_emissions.csv'
    load_vehicle_emissions_csv(vehicle_emissions_csv)
    # summarizations
    basic_data_summarizations(years)
