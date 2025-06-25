import numpy as np
import boto3,io,requests, tarfile, time
from weather_data_credentials import config_setup
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import xarray as xr
from requests.auth import HTTPBasicAuth
import smtplib, os
from email.mime.text import MIMEText
from datetime import datetime

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minIO']['endpoint'],
    aws_access_key_id=config_setup['minIO']['access_key'],
    aws_secret_access_key=config_setup['minIO']['secret_key']
)

bucket_name = config_setup['minIO']['source_bucket']

response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/20250603/00_utc/parquet")
file_list = [obj["Key"] for obj in response.get("Contents", [])]

dictnry = {"plant Name":[],'site lat_lon':[],"Solar Radiation":[],
           "Surface Pressure":[],"u_velocity":[],"v_velocity":[],
           "Temperature":[],"Humidity":[],"Rain Fall":[]}

df_p = pd.read_csv("/home/gopi/doker_airflow/dags/Greenkowindplantdetails_OLD.csv")

for j in file_list:
    print("j",j)
    obj = s3.get_object(Bucket=bucket_name, Key=j)
    body = obj['Body'].read()
    table = pq.read_table(io.BytesIO(body))
    df = table.to_pandas()
    for idx, i in enumerate(df_p['Farm']):
        lat = df_p['latitude'][idx]
        lon = df_p['longitude'][idx]
        if i not in dictnry["plant Name"]:
            dictnry["plant Name"].append(i)
            dictnry["site lat_lon"].append([float(lat),float(lon)])  
        dist = np.sqrt((df['lat'] - lat)**2 + (df['lon'] - lon)**2)
        nearest_idx = dist.idxmin()
        nearest_lat = df.loc[nearest_idx, 'lat']
        nearest_lon = df.loc[nearest_idx, 'lon']
        print(i,nearest_lat,nearest_lon)
        if "rainfall" in j:
            dictnry["Rain Fall"].append([float(nearest_lat),float(nearest_lon)])
        if "radiation" in j:
            dictnry["Solar Radiation"].append([float(nearest_lat),float(nearest_lon)])
        if "pressure" in j:
            dictnry["Surface Pressure"].append([float(nearest_lat),float(nearest_lon)])
        if "humidity" in j:
            dictnry["Humidity"].append([float(nearest_lat),float(nearest_lon)])
        if "temperature" in j:
            dictnry["Temperature"].append([float(nearest_lat),float(nearest_lon)])
        if "u_" in j:
            dictnry["u_velocity"].append([float(nearest_lat),float(nearest_lon)])
        if "v_" in j:
            dictnry["v_velocity"].append([float(nearest_lat),float(nearest_lon)])
for key in dictnry.keys():
    print(len(dictnry[key]))
df = pd.DataFrame(dictnry) 
df.to_csv("/home/gopi/doker_airflow/Weather_data_site_latlon.csv")  

