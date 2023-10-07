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

chunk_size = 50

# Define a generator function to yield chunks of data
def chunked_data_generator():
    for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8'):
        yield chunk_df

# Create a generator object
data_generator = chunked_data_generator()

# Iterate over the generator to get chunks of 50 rows at a time
for chunk in data_generator:
    # Process each chunk here
    print(type(chunk))  # Example: Print the first 5 rows of each chunk


