import folium
import io,boto3
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from weather_data_credentials import config_setup
import xarray as xr
from pymongo import MongoClient
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt

def latlon_plot_on_map(locations,saving_path): # locations are list of dictionaries(lat,lon,name as keys)
    # # Create a map centered on India
    m = folium.Map(location=[22.9734, 78.6569], zoom_start=5)
    for loc in locations:
        folium.Marker(
            location=[loc["lat"], loc["lon"]],
            popup=loc["name"],
            icon=folium.Icon(color='green')
        ).add_to(m)
    m.save(saving_path)
# latlon_plot_on_map(locations,saving_path)

def plotting_df_columns():
    # s3 = boto3.client(
    #     's3',
    #     endpoint_url=config_setup['minIO']['endpoint'],
    #     aws_access_key_id=config_setup['minIO']['access_key'],
    #     aws_secret_access_key=config_setup['minIO']['secret_key']
    # )
    # bucket_name = config_setup['minIO']['source_bucket']
    # response = s3.list_objects_v2(Bucket=bucket_name, Prefix="GEMATS_new/IMD_data/market_wise/2024/")
    # file_list = [obj["Key"] for obj in response.get("Contents", [])]
    # for i in file_list:
    #     if "A1" in i:
    #         print(i)
    #         obj = s3.get_object(Bucket=bucket_name, Key=i)
    #         body = obj['Body'].read()
    #         table = pq.read_table(io.BytesIO(body))
    #         df_IMD = table.to_pandas()
    df_IMD = pd.read_csv("/home/gopi/doker_airflow/IMD/open-meteo/historical/2024/A1.csv")
    df_open_meteo = pd.read_csv("/home/gopi/doker_airflow/IMD/open-meteo/historical/2024/E1.csv")
    # print(df_IMD.columns)
    # print(df_open_meteo)
    plt.figure(figsize=(10, 5))
    # plt.plot(df_IMD.index, df_IMD['rainfall'], label='IMD Rainfall', marker='o')
    # plt.plot(df_open_meteo.index, df_open_meteo['rain_sum'], label='Open-meteo Rainfall', marker='s')
    plt.plot(df_IMD.index, df_IMD['rain_sum'], label='2024 Rainfall', linestyle='-', color='b')
    plt.plot(df_open_meteo.index, df_open_meteo['rain_sum'], label='2025 Rainfall', linestyle='-', color='r')

    plt.xlabel('Days')
    plt.ylabel('Rainfall')
    plt.title('2024 Rain fall: Open-Meteo vs IMD')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("/home/gopi/doker_airflow/dags/GEMATS_scripts/Rainfall_comparison.png", dpi=300)
    print("completed")

plotting_df_columns()