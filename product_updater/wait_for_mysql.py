import os
import logging
import pymysql
from retrying import retry

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

# For local testing
# host = "172.25.184.208"
# user = "mysql"
# password = "mysql"
# database = "buy_online_db"
# port = 3307

# For docker compose
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
port = int(os.getenv('DB_PORT'))

@retry(stop_max_attempt_number=1500, wait_fixed=15000,
       retry_on_exception=lambda e: isinstance(e, pymysql.MySQLError))
def wait_for_mysql():

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
        )
        logger.info("MySQL server is ready!")
        connection.close()
    except pymysql.err.OperationalError as e:
        logger.error("MySQL server is not ready yet.")
        raise

if __name__ == '__main__':
    wait_for_mysql()