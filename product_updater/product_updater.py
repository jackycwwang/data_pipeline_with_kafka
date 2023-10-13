from datetime import datetime
import logging
import os
import pandas as pd
import time
import random

import pymysql
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, TIMESTAMP, MetaData
from sqlalchemy.sql import text

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("product_updater")

# For local testing
# user = 'mysql'
# password = 'mysql'
# hostname =  '172.25.184.208'
# port = '3307'
# database = 'buy_online_db'

# For docker compose
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
hostname = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')


try:
    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{hostname}:{port}/{database}")
    connection = engine.connect()
    logger.info("MySQL connection has been successfully established!")
except pymysql.Error as err:
    logger.error(f"Database connection error: {err}")

# Drop the table if it already existed
tbl_name = 'product'
query = text(f"drop table if exists {tbl_name}")
result = connection.execute(query)
connection.commit()

# Define the metadata
metadata = MetaData()

# Define the product table schema
product_table = Table('product', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(300)),
                      Column('category', String(500)),
                      Column('price', Float),
                      Column('last_updated', TIMESTAMP)
                      )

# Create the table in the database (generates SQL)
metadata.create_all(engine)

# Load the data
file_path = 'data/products_update.csv'
df = pd.read_csv(file_path)
df.fillna("Uncategorized", inplace=True)

# Iterate through your data and insert it into the product table
chunk_size = 15
try:
    with pd.read_csv(file_path, chunksize=chunk_size) as reader:
        for chunk in reader:
            for _, row in chunk.iterrows():
                try:
                    id = row['id']
                    name = row['name']
                    cat = row['category']
                    price = row['price']
                    datetime_obj = datetime.strptime(
                        row['last_updated'], '%Y-%m-%d %H:%M:%S')
                    last_updated = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
                except KeyError as key_err:
                    logger.error(f"KeyError: {key_err}")
                    continue
                except ValueError as value_err:
                    logger.error(f"ValueError: {value_err}")
                    continue
                except Exception as err:
                    logger.error(f"An unexpected error occurred: {err}")
                    continue

                # Insert data into the product table
                insert_query = product_table.insert().values(
                    id=id,
                    name=name,
                    category=cat,
                    price=price,
                    last_updated=last_updated
                )

                # Execute the query
                connection.execute(insert_query)
                connection.commit()
                logger.info(
                    f"Record inserted - id: {id}, name: {name}, category: {cat}, price: {price}, last_updated: {last_updated}")

            # Simulate a random sleep
            time.sleep(random.uniform(1, 10))

except Exception as err:
    logger.error(f"An unexpected exception occurred: {err}")
finally:
    # Close the connection
    connection.close()
