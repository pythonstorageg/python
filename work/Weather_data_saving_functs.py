import pyarrow.compute as pc
import numpy as np
import boto3,io,os,pyarrow,mlflow
from config import config_setup
import pandas as pd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from config import config_setup
import pandas as pd
import xarray as xr
import requests, tarfile, time, glob
from requests.auth import HTTPBasicAuth
from natsort import natsorted 

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

bucket_name = config_setup['source_bucket']

def compute_specific_humidity(temp_c,pressure_pa,rh_percent):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh_percent * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def processing_files(prefix,inputdate,cycle):
    url = config_setup["weather_data"]["url"]
    api_key = config_setup["weather_data"]["api_key"]
    username = config_setup["weather_data"]["username"]
    password = config_setup["weather_data"]["password"]
    files = [
            {'url': url, 'variable': "GreenkoWindEnergy"},
            {'url': url, 'variable': "wind_solar_ind"},  
        ]
    with requests.Session() as session:
        session.auth = HTTPBasicAuth(username, password)     
        for file in files:
            try:
                if 'wind_solar_ind' in file['variable']:
                    continue
                if 'GreenkoWindEnergy' in file['variable']:
                    subdir_name = "GreenkoWindEnergy"
            except Exception as e:
                print(e)
            headers = {
                'inputdate': inputdate,
                'cycle': cycle,
                'datavariable': subdir_name,
                'api-key': api_key,
            }
            try: 
                response = session.post(file['url'], headers=headers, stream=True)
            except Exception as e:
                print(e)
                continue
            print("response",response.status_code)
            if response.status_code == 200:
                archive_bytes = io.BytesIO(response.content)
                archive_bytes.seek(0)  # Reset stream
                print("Extrating zip file")
                with tarfile.open(fileobj=archive_bytes, mode='r:gz') as tar:
                    data_dict = {}
                    for member in tar.getmembers():
                        # try:
                            if member.isfile():
                                file_name = ("_").join(member.name.split("_")[:-1])
                                extracted_file = tar.extractfile(member)
                                file_data = extracted_file.read()
                                file_data = io.BytesIO(file_data)
                                ds = xr.open_dataset(file_data)
                                
                                buffer = io.BytesIO()
                                data_bytes = ds.to_netcdf(buffer) 
                                # s3.put_object(Bucket=bucket_name, Key=prefix+"/"+str(inputdate)+"/nc/"+member.name, Body=data_bytes)

                                # ds['time'] = (
                                # pd.to_datetime(ds['time'].values)
                                # .tz_localize('UTC')
                                # .tz_convert('Asia/Kolkata')
                                # .strftime('%Y-%m-%d %H:%M:%S')
                                # )

                                ds.to_netcdf("/home/gopi/doker_airflow/20May2025_00_origi/"+member.name)

                    #             df = ds.to_dataframe().reset_index()
                        
                    #             print(file_name,"apeending to dict")
                    #             if file_name in data_dict:
                    #                 data_dict[file_name] = pd.concat([data_dict[file_name], df], ignore_index=True)
                    #             else:
                    #                 data_dict[file_name] = df      
                    #     except:
                    #         continue    
                    # df_plant = pd.read_csv("Greenkowindplantdetails_OLD.csv")  
                    # for idx, plant_name in enumerate(df_plant["Farm"]):
                    #     plant_lat = df_plant["latitude"][idx]
                    #     plant_lon = df_plant["longitude"][idx]
                    #     plant_height = df_plant["Hhgt"][idx]
                    #     print("Processing",idx,plant_name,plant_lat,plant_lon,plant_height)
                    #     plant_data = None
                    #     for file_name in data_dict.keys():
                    #         dist = np.sqrt((data_dict[file_name]['lat'] - plant_lat)**2 + (data_dict[file_name]['lon'] - plant_lon)**2)
                    #         nearest_idx = dist.idxmin()
                    #         nearest_lat = data_dict[file_name].loc[nearest_idx, 'lat']
                    #         nearest_lon = data_dict[file_name].loc[nearest_idx, 'lon']
                    #         nearest_rows = data_dict[file_name][(data_dict[file_name]['lat'] == nearest_lat) & (data_dict[file_name]['lon'] == nearest_lon)]
                    #         if "temperature" in file_name:
                    #             nearest_rows.loc[:, 't2m'] = (nearest_rows['t2m'] - 273).round(2)
                    #         if "u_wind" in file_name:
                    #             dist = np.sqrt((data_dict[file_name]['lat'] - plant_lat)**2 + (data_dict[file_name]['lon'] - plant_lon)**2+
                    #                            (data_dict[file_name]['lev'] - plant_height)**2)
                    #             nearest_idx = dist.idxmin()
                    #             nearest_lat = data_dict[file_name].loc[nearest_idx, 'lat']
                    #             nearest_lon = data_dict[file_name].loc[nearest_idx, 'lon']
                    #             nearest_lev = data_dict[file_name].loc[nearest_idx, 'lev']
                    #             nearest_rows = data_dict[file_name][(data_dict[file_name]['lat'] == nearest_lat) & (data_dict[file_name]['lon'] == nearest_lon)
                    #                                                 &(data_dict[file_name]['lev'] == nearest_lev)].copy()
                    #             # nearest_rows = nearest_rows[nearest_rows['lev']==50].copy()
                    #             nearest_rows.drop(columns='lev', inplace=True)
                    #         #    nearest_rows = interpolate_velocity(nearest_rows,plant_height,'u')
                    #         if "v_wind" in file_name:
                    #             dist = np.sqrt((data_dict[file_name]['lat'] - plant_lat)**2 + (data_dict[file_name]['lon'] - plant_lon)**2+
                    #                            (data_dict[file_name]['lev'] - plant_height)**2)
                    #             nearest_idx = dist.idxmin()
                    #             nearest_lat = data_dict[file_name].loc[nearest_idx, 'lat']
                    #             nearest_lon = data_dict[file_name].loc[nearest_idx, 'lon']
                    #             nearest_lev = data_dict[file_name].loc[nearest_idx, 'lev']
                    #             nearest_rows = data_dict[file_name][(data_dict[file_name]['lat'] == nearest_lat) & (data_dict[file_name]['lon'] == nearest_lon)
                    #                                                 &(data_dict[file_name]['lev'] == nearest_lev)].copy()
                    #             # nearest_rows = nearest_rows[nearest_rows['lev']==50].copy()
                    #             nearest_rows.drop(columns='lev', inplace=True)
                    #         #    nearest_rows = interpolate_velocity(nearest_rows,plant_height,'v')
                    #         if plant_data is None:
                    #             plant_data = nearest_rows
                    #         else:
                    #             col_name = nearest_rows.columns[3]
                    #             plant_data = plant_data.copy()
                    #             plant_data.loc[:, col_name] = nearest_rows.loc[:, col_name].values       
                    #     plant_data = plant_data.reset_index(drop=True)
                    #     plant_data['rh2m'] = compute_specific_humidity(plant_data['t2m'], plant_data['press'], plant_data['rh2m']) 
                    #     plant_data['rf'] = plant_data['rf'].round(2)
                    #     plant_data['WindSpeed_50m(m/s)'] = np.sqrt(plant_data['u']**2 + plant_data['v']**2).round(2)
                    #     plant_data['WindDirection_50m(deg)'] = (np.arctan2(-plant_data['u'], -plant_data['v']) * (180 / np.pi)) % 360
                    #     plant_data = plant_data.drop(columns=['u', 'v'])
                    #     plant_data.rename(columns={
                    #     'press': 'Pressure(Pa)',
                    #     'dswrf': 'GHI(W/m^2)',
                    #     'rf':'Rainfall(kg/m^2)',
                    #     'rh2m':'SpecificHumidity(kg/kg)'
                    #     }, inplace=True)
                    #     plant_data.to_csv("/home/gopi/doker_airflow/15may2025/scripts/saved_files/1.csv")
                    #     table = pa.Table.from_pandas(plant_data)
                    #     buffer = io.BytesIO()
                    #     pq.write_table(table, buffer)
                    #     buffer.seek(0)
                    #     # s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/Plant_wise_data/"+plant_name+".parquet",Body=buffer.getvalue())
                    #     break
                    # # for file_name in data_dict.keys():
                    # #     table = pa.Table.from_pandas(data_dict[file_name])
                    # #     buffer = io.BytesIO()
                    # #     pq.write_table(table, buffer)
                    # #     buffer.seek(0)
                    # #     s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue())
                    #     # break


t1 = time.time()
processing_files("weather_data/processed","20250520","00")
print(time.time()-t1)