import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
import glob, boto3,io
from config import config_setup
import pandas as pd
import pyarrow.parquet as pa
from natsort import natsorted
import xarray as xr

bucket_name = config_setup['source_bucket']

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

def day_wise_data(date):
    # table = pq.read_table("/home/gopi/doker_airflow/parquet_files/surface_pressure_R1.parquet")

# Approach - 1
    # lat = table.column("lat")
    # lon = table.column("lon")

    # lat_lower = plant_lat - 0.5
    # lat_upper = plant_lat + 0.5
    # lon_lower = plant_lon - 0.25
    # lon_upper = plant_lon + 0.25

    # mask = (
    #     pc.and_(
    #         pc.and_(pc.greater(lat, lat_lower), pc.less(lat, lat_upper)),
    #         pc.and_(pc.greater(lon, lon_lower), pc.less(lon, lon_upper))
    #     )
    # )

    # filtered_table = table.filter(mask)
    # filtered_df = filtered_table.to_pandas()
    # print(filtered_df.head(24))
    # grouped_df = filtered_df.groupby("time", as_index=False)["press"].mean()
    # print(grouped_df)

# Approach - 2

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/"+str(date)+"/parquet")
    lst = [obj["Key"] for obj in response.get("Contents", [])]   
    dictnry = {} 
    for i in lst:
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read()
        table = pa.read_table(io.BytesIO(body))
        dictnry[i.split("/")[-1]] = table
    print(dictnry)
    df = pd.read_csv("Greenkowindplantdetails_OLD.csv")
    for idx, plant_name in enumerate(df["Farm"]):
        plant_lat = df["latitude"][idx]
        plant_lon = df["longitude"][idx]
        combined_table = None
        nearest_lat = None
        nearest_lon = None
        for parameter in dictnry.keys():
            table = dictnry[parameter]
            lat_diff = pc.abs(pc.subtract(table["lat"], plant_lat))
            lon_diff = pc.abs(pc.subtract(table["lon"], plant_lon))
            total_diff = pc.add(lat_diff, lon_diff)
            min_index = np.argmin(total_diff.to_numpy())
            nearest_row = table.slice(min_index, 1)
            nearest_lat = nearest_row.column("lat")[0].as_py()
            nearest_lon = nearest_row.column("lon")[0].as_py()
            lat_mask = pc.equal(table["lat"], nearest_lat)
            lon_mask = pc.equal(table["lon"], nearest_lon)
            combined_mask = pc.and_(lat_mask, lon_mask)

            filtered_table = table.filter(combined_mask)
            if combined_table == None:
                combined_table = filtered_table
            else:
                new_column = filtered_table.column(3)
                new_column_name = filtered_table.schema.names[3]
                combined_table = combined_table.append_column(new_column_name, new_column)
    # print(combined_table)  

day_wise_data(20250509)

