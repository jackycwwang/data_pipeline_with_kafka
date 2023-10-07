import pandas as pd
from sqlalchemy import create_engine, text

# remote_url="172.25.184.208"
remote_url="localhost"
engine = create_engine(f"mysql+pymysql://mysql:mysql@{remote_url}:3307/buy_online_db")

connection = engine.connect()

# Execute a query to fetch the list of databases
query = text("SHOW DATABASES")
result = connection.execute(query)

# Iterate through the result and print the database names
# for row in result:
    # print(row[0])

file_path = './data/1429_1.csv'
chunk_size = 10

with pd.read_csv(file_path, chunksize=chunk_size) as reader:
    # print(reader)
    for chunk in reader:
        print(chunk)
