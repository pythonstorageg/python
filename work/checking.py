import pandas as pd
import numpy as np
import xarray as xr
import os
from datetime import datetime as dt, timedelta
import traceback
import math
import sys
import logging
import glob
import os
import json
import boto3,io
from config import config_setup

# s3 = boto3.client(
#     's3',
#     endpoint_url=config_setup['minio_endpoint'],
#     aws_access_key_id=config_setup['access_key'],
#     aws_secret_access_key=config_setup['secret_key']
# )

# bucket_name = config_setup['source_bucket']

# response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/20250519/nc")
# lst = [obj["Key"] for obj in response.get("Contents", [])]


# df = None
# for i in lst:
#     if "u_wind" in i:
#         obj = s3.get_object(Bucket=bucket_name, Key=i)
#         body = obj['Body'].read()
#         ds = xr.open_dataset(io.BytesIO(body))
#         df_ds = ds.to_dataframe().reset_index()
#     if "v_wind" in i:
#         obj = s3.get_object(Bucket=bucket_name, Key=i)
#         body = obj['Body'].read()
#         ds1 =  xr.open_dataset(io.BytesIO(body))
        

# plant_latitude = "14.815438"
# plant_longitude = "77.210792"
# level = 90

# # ds = xr.open_dataset("/home/gopi/doker_airflow/15may2025/scripts/processed/u_wind_R1.nc")
# u_data = ds['u'].sel(lat=plant_latitude, lon=plant_longitude, lev=level, method='nearest')
# u_data1 = ds['u'].sel(lat=plant_latitude, lon=plant_longitude, method='nearest')
# df1 = u_data.to_dataframe().reset_index()
# df3 = u_data1.to_dataframe().reset_index()
# # df1 = pd.DataFrame({'time': ds.time.values,f'U Wind_{level}m': u_data.values})

# # ds = xr.open_dataset("/home/gopi/doker_airflow/15may2025/scripts/processed/v_wind_R1.nc")
# v_data = ds1['v'].sel(lat=plant_latitude, lon=plant_longitude, lev=level, method='nearest')
# v_data1 = ds1['v'].sel(lat=plant_latitude, lon=plant_longitude, method='nearest')
# df2 = v_data.to_dataframe().reset_index()
# df4 = v_data1.to_dataframe().reset_index()

# filtered_df1 = df1[
#    (df1["time"] == "2025-05-19 06:30:00") 
#     # & (df1["lon"]==77.255859) & (df1["lat"]==14.824219)
# ]

# filtered_df2 = df2[
#    (df2["time"] == "2025-05-19 06:30:00") 
#     # & (df1["lon"]==77.255859) & (df1["lat"]==14.824219)
# ]

# print(filtered_df1)
# print(filtered_df2)


# filtered_df1 = df3[
#    (df3["time"] == "2025-05-19 06:30:00") 
#     # & (df1["lon"]==77.255859) & (df1["lat"]==14.824219)
# ]

# filtered_df2 = df4[
#    (df4["time"] == "2025-05-19 06:30:00") 
#     # & (df1["lon"]==77.255859) & (df1["lat"]==14.824219)
# ]

# print(filtered_df1)
# print(filtered_df2)



# dist = np.sqrt((df_ds['lat'] - plant_lat)**2 + (df_ds['lon'] - plant_lon)**2+
#                                                (df_ds['lev'] - level)**2)
# nearest_idx = dist.idxmin()
# nearest_lat = df_ds.loc[nearest_idx, 'lat']
# nearest_lon = df_ds.loc[nearest_idx, 'lon']
# nearest_lev = df_ds.loc[nearest_idx, 'lev']
# nearest_rows = df_ds[(df_ds['lat'] == nearest_lat) & (df_ds['lon'] == nearest_lon)
#                                     &(df_ds['lev'] == nearest_lev)].copy()
# print("nearest_rows",nearest_rows)

plant_lat = "14.815438"
plant_lon = "77.210792"
level = 50

ds = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/u_wind_R1.nc")
ds1 = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/2m_relative_humidity_R1.nc")

df1 = ds.to_dataframe().reset_index()
df2 = ds1.to_dataframe().reset_index()
print(df1.head(10))
print(df2.head(10))
# df1.to_csv("/home/gopi/doker_airflow/20May2025_00/df1.csv")
# filtered_df1 = df1[
#    (df1["time"] == "2025-05-21 00:00:00") 
#    & ((df1["lon"]>77)&(df1["lon"]<78)) & ((df1["lat"]>14)&(df1["lat"]<15))
# ].copy()

# df2 = ds1.to_dataframe().reset_index()
# filtered_df2 = df2[
#    (df2["time"] == "2025-05-21 00:00:00") 
#    & ((df2["lon"]>77)&(df2["lon"]<78)) & ((df2["lat"]>14)&(df2["lat"]<15))
# ].copy()

# filtered_df1.loc[:, 'v'] = filtered_df2.loc[:, 'v'].values

# filtered_df1.loc[:, 'speed'] = np.sqrt(filtered_df1['u']**2 + filtered_df1['v']**2)

# print(filtered_df1)

# filtered_df1.to_csv("/home/gopi/doker_airflow/20May2025_00/speed.csv")




# u_data = ds['u'].sel(lat=plant_lat, lon=plant_lon, lev=level, method='nearest')
# df1 = u_data.to_dataframe().reset_index()
# df1 = ds.to_dataframe().reset_index()
# filtered_df1 = df1[
#    (df1["time"] == "2025-05-21 00:00:00") 
#    & ((df1["lon"]>77)&(df1["lon"]<78)) & ((df1["lat"]>14)&(df1["lat"]<15))
# ]
# print(filtered_df1)
# filtered_df1.to_csv("/home/gopi/doker_airflow/20May2025_00/u_77781415.csv")
# u = filtered_df1['u'].iloc[0]

# ds = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/v_wind_R1.nc")
# v_data = ds['v'].sel(lat=plant_lat, lon=plant_lon, lev=level, method='nearest')
# df2 = v_data.to_dataframe().reset_index()
# filtered_df2 = df2[
#    (df2["time"] == "2025-05-21 00:00:00") 
# ]
# print(filtered_df2)

# v = filtered_df2['v'].iloc[0]

# print(u,v)
# speed = np.sqrt(u**2 + v**2)
# print("speed",speed)

# ds = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/2m_relative_humidity_R1.nc")
# u_data = ds['u'].sel(lat=plant_lat, lon=plant_lon, lev=level, method='nearest')
# df1 = u_data.to_dataframe().reset_index()
# filtered_df1 = df1[
#    (df1["time"] == "2025-05-21 00:00:00") 
# ]
# print(filtered_df1)
# u = filtered_df1['u'].iloc[0]

