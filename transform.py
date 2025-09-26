import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)


# Call all methods from transform.py here
if __name__ == "__main__":
    print("hi")
