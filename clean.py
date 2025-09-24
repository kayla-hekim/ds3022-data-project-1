import duckdb
import logging



logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)



# remove duplicates
def remove_duplicates(year_range=(2024,2025)):
    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB, ready to remove duplicates")

        # con.execute("SET schema='tlc';")

        colors = ["yellow", "green"]
        tables = []
        for year in year_range:
            for color in colors:
                for month in range(1,13):
                    table = f"{color}_{year}_{month:02d}"
                    tables.append(table)
            

        for each_table in tables:
            con.execute(f"""
                CREATE TABLE {each_table}_clean AS 
                SELECT DISTINCT * FROM {each_table};
                        
                DROP TABLE {each_table};
                ALTER TABLE {each_table}_clean RENAME TO {each_table};
            """)

    except Exception as e:
        # print(f"An error occurred for yellow green taxi parquet loading: {e}")
        logger.error(f"An error occurred yellow green taxi parquet loading: {e}")


def zero_passengers_removed():
    con = None


def zero_miles_removed():
    con = None


def more_100mi_removed():
    con = None


def more_24hr_removed():
    con = None


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