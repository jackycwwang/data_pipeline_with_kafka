import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("mysql+pymysql://mysql:mysql@localhost:3307/buy_online_db")

connection = engine.connect()

# Execute a query to fetch the list of databases
query = text("SHOW DATABASES")
result = connection.execute(query)

# Iterate through the result and print the database names
for row in result:
    print(row[0])


# df = pd.read_csv("")