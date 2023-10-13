import os
import logging
import mysql.connector
from retrying import retry


log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


# For local testing
# user = 'mysql'
# password = 'mysql'
# host =  '172.25.184.208'
# port = 3307
# database = 'buy_online_db'

# For docker compose
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
port = int(os.getenv('DB_PORT'))

# retry_on_exception=lambda e: isinstance(e, mysql.connector.Error)


@retry(
    stop_max_attempt_number=1500, wait_fixed=15000)
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
        )
        logger.info("MySQL server is ready!")
        conn.close()
    except mysql.connector.Error as e:
        logger.error("MySQL server is not ready yet.")
        raise


if __name__ == '__main__':
    connect_to_mysql()
