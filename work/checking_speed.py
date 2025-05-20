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

ds = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/u_wind_R1.nc")
ds1 = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/u_wind_R2.nc")
df = ds.to_dataframe().reset_index()
df1 = ds1.to_dataframe().reset_index()


ds2 = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/v_wind_R1.nc")
ds3 = xr.open_dataset("/home/gopi/doker_airflow/20May2025_00/v_wind_R2.nc")
df2 = ds2.to_dataframe().reset_index()
df3 = ds3.to_dataframe().reset_index()

df_u = pd.concat([df, df1], ignore_index=True)
df_v = pd.concat([df2, df3], ignore_index=True)

df_u.loc[:, 'v'] = df_v.loc[:, 'v'].values

df_u.loc[:, 'speed'] = np.sqrt(df_u['u']**2 + df_u['v']**2)

filtered_df = df_u[
   (df_u["time"] == "2025-05-21 00:00:00") 
#    & ((df_u["speed"]>3.1)&(df_u["speed"]<3.2))
]

filtered_df.to_csv("/home/gopi/doker_airflow/20May2025_00/speed.csv")
# print(filtered_df)


# logging.info(f"Detected NetCDF fls: {len(fls_ncdf)} found")
# ext_fl = {}  
# fls_ncdf = [f for f in glob.glob(os.path.join("/home/gopi/doker_airflow/20May2025_00_origi/", "*.nc")) if f.endswith(".nc")]
# print(fls_ncdf)
# for fl in fls_ncdf:
#     try:
#         ds = xr.open_dataset(fl)
#         ltk = 'lat' if 'lat' in ds.variables else 'latitude' if 'latitude' in ds.variables else None
#         lnk = 'lon' if 'lon' in ds.variables else 'longitude' if 'longitude' in ds.variables else None
#         if not ltk or not lnk:
#             logging.info(f"{fl} - Not found")
#             ds.close()
            
#         minlat, maxlat = ds[ltk].min().item(), ds[ltk].max().item()
#         minlon, maxlon = ds[lnk].min().item(), ds[lnk].max().item()
#         ext_fl[fl] = (minlat, maxlat, minlon, maxlon)
#     except:
#         continue
# print(ext_fl)   
# plant_lat = "14.815438"
# plant_lon = "77.210792" 
# wind_levels = [50,80,100,120,150]  
# for level in wind_levels:
#     print("level@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",level)
#     u_nc, v_nc = None, None

#     for fl in fls_ncdf:
#         try:
#             ds = xr.open_dataset(fl)
#             if 'u' in ds.variables:
#                 u_nc = fl
#             if 'v' in ds.variables:
#                 v_nc = fl
#             ds.close()
#             if u_nc and v_nc:
#                 break
#         except Exception as e:
#             logging.error(f"Error opening {fl}:",exc_info=True)
#     try:
#         ds = xr.open_dataset(u_nc)
#         print("unc",u_nc)
#         u_data = ds['u'].sel(lat=plant_lat, lon=plant_lon, lev=level, method='nearest')
#         df = u_data.to_dataframe().reset_index()
#         u_time_series = pd.DataFrame({'time': ds.time.values, f'U Wind_{level}m': u_data.values})
#         # filtered_df = u_time_series[
#         #     (u_time_series["time"] == "2025-05-21 00:00:00") 
#         #     ]
#         # print(filtered_df)
#         ds.close()
#     except Exception as e:
#         logging.error(f"Error extracting U Wind at {level}m:",exc_info=True)
#         continue

#     try:
#         ds = xr.open_dataset(v_nc)
#         print("vnc",v_nc)
#         v_data = ds['v'].sel(lat=plant_lat, lon=plant_lon, lev=level, method='nearest')
#         df = u_data.to_dataframe().reset_index()
#         v_time_series = pd.DataFrame({'time': ds.time.values, f'V Wind_{level}m': v_data.values})
#         # filtered_df = v_time_series[
#         #     (v_time_series["time"] == "2025-05-21 00:00:00") 
#         #     ]
#         # print(filtered_df)
#         ds.close()
#     except Exception as e:
#         logging.error(f"Error extracting V Wind at {level}m:",exc_info=True)
#         continue

#     # # Merge U and V wind data
#     wind_df = pd.merge(u_time_series,v_time_series, on='time')
#     wind_df["time"] = pd.to_datetime(wind_df["time"]).dt.tz_localize("UTC")
#     wind_df["DateTime"] = wind_df["time"].dt.tz_convert("Asia/Kolkata")
#     wind_df.set_index('DateTime',inplace=True)
#     wind_df.index = wind_df.index.tz_localize(None)
#     wind_df[f'WindSpeed_{level}m(m/s)'] = np.sqrt(wind_df[f'U Wind_{level}m']**2 + wind_df[f'V Wind_{level}m']**2)
#     filtered_df = wind_df[
#     (wind_df["time"] == "2025-05-20 18:30:00+00:00") 
#     ]
#     print(filtered_df)
#     wind_df = wind_df.asfreq("15min")
#     wind_df.reset_index(inplace=True,drop=False)
#     wind_df.to_csv("/home/gopi/doker_airflow/20May2025_00/repliate"+str(level)+".csv")
#     filtered_df = wind_df[
#     (wind_df["time"] == "2025-05-20 18:30:00+00:00") 
#     ]
#     print(filtered_df)
#     wind_df.drop(columns=["time"],inplace=True)
           
