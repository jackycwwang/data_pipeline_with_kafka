import os
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, TIMESTAMP, MetaData
from sqlalchemy.sql import text
import pymysql
from datetime import datetime
import logging


logger = logging.getLogger("my_logger")

# host_name =  "172.25.184.208"
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host_name =  os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

try:
  engine = create_engine(f"mysql+pymysql://{user}:{password}@{host_name}:{port}/{database}")
  connection = engine.connect()
except pymysql.Error:
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
file_path = 'data/marketing_sample_for_walmart_com-ecommerce__20191201_20191231__30k_data.csv'
df = pd.read_csv(file_path)
df.fillna("Uncategorized", inplace=True)

# Insert each row into the table product
try:
    for id, row in df.iterrows():
        try:
            uniq_id = str(id+1)
            # print(f"id={id}")
            name = row['Product Name']
            cat = row['Category']
            price = row['List Price']
            datetime_str = row['Crawl Timestamp']
            datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S %z')
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
            id=uniq_id,
            name=name,
            category=cat,
            price=price,
            last_updated=last_updated
        )

        # Execute the query
        connection.execute(insert_query)
        connection.commit()

except Exception as err:
    logger.error(f"An unexpected error occurred: {err}")
finally:
    # Close the connection
    connection.close()
