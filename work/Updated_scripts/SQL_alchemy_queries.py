from sqlalchemy import create_engine,text,types, inspect
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime as dt
import yaml,os,pytz

def getting_data_from_db(engine,table,start_time,end_time):
    query = text(f"""
    SELECT *
    FROM "{table}"
    WHERE "TIMESTAMP" >= '{start_time}'
    AND "TIMESTAMP" <= '{end_time}';""")
    with engine.connect() as conn:
        df_db = pd.read_sql(query, conn)
    df_db = df_db.sort_values('TIMESTAMP').reset_index(drop=True)
    df_db["TIMESTAMP"] = pd.to_datetime(df_db["TIMESTAMP"],utc=True)
    df_db["TIMESTAMP"] = df_db["TIMESTAMP"].dt.tz_convert('Asia/Kolkata')
    df_db["TIMESTAMP"] = df_db["TIMESTAMP"].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df_db

def upload_df_to_timescale(engine,df,table_name):
    df['TIMESTAMP'] = pd.to_datetime(
        df['TIMESTAMP'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    ).dt.tz_localize('Asia/Kolkata')
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace', 
        index=False,
        dtype={
            'TIMESTAMP': types.TIMESTAMP(timezone=True)
        }
    )

def max_time_stamp(engine,table):
    query = text(f"""
        SELECT MAX("TIMESTAMP") AS max_timestamp
        FROM "{table}"
        """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        print(result)
        return result[0].astimezone(pytz.timezone('Asia/Kolkata'))
        

def plotting_xy_scatter(df,x,y):
    plt.scatter(df[x], df[y])
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f'Scatter Plot of {x} vs {y}')
    plt.grid(True)
    plt.savefig('scatter_plot.jpg', format='jpg', dpi=300)  # dpi=300 for high resolution

def creating_hyper_table(engine,table_name,timestamp_column,chunk_interval):
    hypertable_sql = text(f"""
    SELECT create_hypertable(
        '{table_name}',
        '{timestamp_column}',
        chunk_time_interval => INTERVAL '{chunk_interval}',
        migrate_data => true
    );
    """)

    # Execute the SQL
    with engine.connect() as conn:
        conn.execute(hypertable_sql)
        conn.commit()
    print("hyper table created")

def size_of_table(engine,table_name):
    query = text(f"""
    SELECT
        pg_size_pretty(pg_total_relation_size('{table_name}')) AS total_size,
        pg_size_pretty(pg_table_size('{table_name}')) AS table_only_size,
        pg_size_pretty(pg_indexes_size('{table_name}')) AS index_size
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name}).fetchone()
        print(f"Total size     : {result[0]}")
        print(f"Table size     : {result[1]}")
        print(f"Indexes size   : {result[2]}")

def list_the_tables(engine,table_name):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Available tables:", tables)

    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' does not exist.")


engine = create_engine("postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database")
# res = max_time_stamp(engine,"Daily_data")
# res = getting_data_from_db(engine,"Daily_data","2025-06-14 00:00:00","2025-06-23 23:59:59")
# upload_df_to_timescale(engine,res,"test")
# print(res)
# creating_hyper_table(engine,"test","TIMESTAMP",'1 day')
size_of_table(engine,'Daily_data')
# list_the_tables(engine,"Daily_data")