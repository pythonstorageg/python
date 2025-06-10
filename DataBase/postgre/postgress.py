import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect


connection_string = 'postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database'

engine = create_engine(connection_string)

# # Query from existing ta
# # query = "SELECT * FROM processed_files"

# # with engine.connect() as conn:
# #     df = pd.read_sql(text(query), conn)
# # # df.to_csv("/home/ggadmin/weather_data/landing_zone/processed_files_13_30.csv")
# # print(df)

# # Adding the table
# df = pd.read_csv("/home/gopi/git_personal/python/DataBase/postgre/gis.csv")
# table_name = 'ap01_psp'
# df.to_sql(table_name, engine, index=False, if_exists='replace') 
# print("table added")

# # # Delete rows in table
# # with engine.connect() as conn:
# #     conn.execute(text("TRUNCATE TABLE \"sample dataframe\""))
# #     conn.commit()

# Drop the table
# with engine.connect() as conn:
#     conn.execute(text('DROP TABLE IF EXISTS "test_table"'))
#     conn.commit()
# print("Drop the table")

# list the tables
inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables:", tables)






