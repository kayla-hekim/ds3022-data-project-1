import duckdb
import logging
import subprocess
import sys
from pathlib import Path

# CHOSE TO DO DBT METHOD - THIS IS LOADING THE COMMAND AND USING IT
# NO USE OF DUCKDB SCRIPT UNLESS YOU'RE TESTING WITH qa_print() OR export() METHODS COMMENTED OUT BELOW 


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)


# wonky file stuff
PROJECT_DIR = Path(__file__).resolve().parent
DBT_DIR = PROJECT_DIR / "dbt"   
DB_PATH = PROJECT_DIR / "emissions.duckdb"

# subprocess document to call CLI: https://stackoverflow.com/questions/4364087/python-subprocess-using-import-subprocess
# call the dbt command (might be the same neal and the TA uses?)
def run_dbt():
    cmd = ["dbt", "build", "-s", "+data_transformation", "--profiles-dir", "."]
    try:
        subprocess.run(cmd, check=True, cwd=str(DBT_DIR))
        logger.info("subprocess worked! DBT ran with transform command 'dbt build --profiles-dir .'")
    except subprocess.CalledProcessError as e:
        sys.exit(f"dbt build failed: {e}")
        logger.warning(f"DBT build failed: {e}")


# testing nulls of mph and co2 columns
# def qa_print():
#     con = duckdb.connect(str(DB_PATH))
    
#     try:
#         qa = con.execute("""
#             SELECT
#             COUNT(*) AS rows_total,
#             SUM(CASE WHEN mph_per_trip IS NULL THEN 1 ELSE 0 END) AS mph_nulls,
#             SUM(CASE WHEN co2_kg_per_trip IS NULL THEN 1 ELSE 0 END) AS co2_nulls
#             FROM data_transformation
#         """).df()
#         print(qa)
#         logger.info(f"able to run qa to test mph and co2 columns: {qa}")
#     except Exception as e:
#         print(f"error in executing null tests for either mph or co2 columns")
#         logger.warning(f"error in executing null tests for either mph or co2 columns")


# outputting data_transform table to a csv data_transformation_sample.csv
# def export():
#     con = duckdb.connect(str(DB_PATH))

#     try:
#         con.execute("""
#             COPY (SELECT * FROM data_transformation)
#             TO 'data_transformation_sample.csv' (HEADER, DELIMITER ',');
#             """
#         )
#         print(f"Wrote data_transformation_sample.csv")
#         logger.info(f"Wrote data_transformation_sample.csv")
#     except Exception as e:
#         print(f"error in outputting data_transformation as a csv")
#         logger.warning(f"error in outputting data_transformation as a csv")


# Call all methods from transform.py here
if __name__ == "__main__":
    run_dbt() # runs "dbt build -s +data_transformation --profiles-dir ."

    # testing transform worked
    # qa_print()
    # export()
