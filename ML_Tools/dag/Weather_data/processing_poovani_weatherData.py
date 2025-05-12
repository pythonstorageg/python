import pyarrow.compute as pc
import numpy as np
import boto3,io,os
from config import config_setup
import pandas as pd
import pyarrow.parquet as pa
import pyarrow, logging
from datetime import datetime

logger = logging.getLogger("airflow.task")

bucket_name = config_setup['source_bucket']

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

def interpolate_values(table, height, lat, lon):
    groups = table.column("time").to_pylist()
    x_col = table.column("lev").to_numpy()
    try:
        h_col = table.column("u").to_numpy()
        vel = 'u'
    except:
        h_col = table.column("v").to_numpy()
        vel = 'v'
    unique_groups = np.unique(groups)
    group_indices = {group: np.where(np.array(groups) == group)[0] for group in unique_groups}
    result = []
    for group in unique_groups:
        indices = group_indices[group]
        x_group = x_col[indices]
        h_group = h_col[indices]
        h_interp = float(np.interp(height, x_group, h_group))
        result.append({
            "group": group,
            "lat": lat,
            "lon": lon,
            "x": height,
            "h": h_interp
        })
    columns = {
        "time": [row["group"] for row in result],
        "lat": [row["lat"] for row in result],
        "lon": [row["lon"] for row in result],
        vel: [row["h"] for row in result]
    }
    result_table = pyarrow.table(columns)
    return result_table

def plant_wise_data_process(date):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/"+str(date)+"/parquet")
    lst = [obj["Key"] for obj in response.get("Contents", [])]   
    dictnry = {} 
    parameter_name_new = {"time":"time","lat":"latitude","lon":"longitude","rf":"Rain_Fall","dswrf":"Solar_Radiation","rh2m":"Relative_Humidity","press":"Pressure","t2m":"Temperature",
                          "u":"u_velocity","v":"v_velocity"}
    print("Getting parquet files data from MinIO")
    for i in lst:
        print("getting data:",i)
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read()
        table = pa.read_table(io.BytesIO(body))
        dictnry[i.split("/")[-1]] = table
        print("Data adding to dictnry")
    print("Reading sites lat lon values")
    dag_folder = os.path.dirname(__file__)
    df = pd.read_csv(dag_folder+"/Greenkowindplantdetails_OLD.csv")
    for idx, plant_name in enumerate(df["Farm"]):
        plant_lat = df["latitude"][idx]
        plant_lon = df["longitude"][idx]
        plant_height = df["Hhgt"][idx]
        print("Processing",plant_name,plant_lat,plant_lon)
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
            if "_wind" in parameter:
                filtered_table = interpolate_values(filtered_table,plant_height,nearest_lat,nearest_lon)
            if combined_table == None:
                combined_table = filtered_table
            else:
                new_column = filtered_table.column(3)
                new_column_name = filtered_table.schema.names[3]
                combined_table = combined_table.append_column(new_column_name, new_column)
        if combined_table:
            old_names = combined_table.schema.names        
            new_names = [parameter_name_new.get(name, name) for name in old_names] 
            combined_table = combined_table.rename_columns(new_names)    
            buffer = io.BytesIO()
            pa.write_table(combined_table, buffer)
            buffer.seek(0)
            s3.put_object(Bucket=bucket_name,Key="weather_data/processed/"+str(date)+"/Plant_wise_data/"+plant_name+"_"+str(plant_lat)+"_"+str(plant_lon)+".parquet",Body=buffer.getvalue())        