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

def send_mail(message):
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(config_setup['mail_cred']['userName'], config_setup['mail_cred']['appPasword'])
    message = f"<b> {message} </b>"
    msg = MIMEText(message,'html')
    msg['Subject'] = "Retrieving weather data"
    msg['From'] = config_setup['mail_cred']['userName']
    msg['To'] = ", ".join(config_setup['mail_cred']['to_recipients'])
    msg['Cc'] = ", ".join(config_setup['mail_cred']['cc_recipients']) 
    server.sendmail(config_setup['mail_cred']['userName'],config_setup['mail_cred']['to_recipients']+config_setup['mail_cred']['cc_recipients'],msg.as_string())

def checking_date_minIO(prefix):
    if s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix).get("Contents"):
        return "file_exists"
    return "file_missing"

def compute_specific_humidity(temp_c,pressure_pa,rh_percent):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh_percent * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def processing_files(prefix,inputdate,cycle):
    print("input date",inputdate,cycle)
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_folder, "Greenkowindplantdetails_OLD.csv")
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
                        try:
                            if member.isfile():
                                file_name = ("_").join(member.name.split("_")[:-1])
                                extracted_file = tar.extractfile(member)
                                file_data = extracted_file.read()
                                file_data = io.BytesIO(file_data)
                                ds = xr.open_dataset(file_data)
                                
                                buffer = io.BytesIO()
                                data_bytes = ds.to_netcdf(buffer) 

                                ds['time'] = (
                                pd.to_datetime(ds['time'].values)
                                .tz_localize('UTC')
                                .tz_convert('Asia/Kolkata')
                                .strftime('%Y-%m-%d %H:%M:%S')
                                )

                                s3.put_object(Bucket=bucket_name, Key=prefix+"/"+str(inputdate)+"/"+str(cycle)+"_utc"+"/nc/"+member.name, Body=data_bytes)

                                df = ds.to_dataframe().reset_index()
                        
                                print(file_name,"apending to dict")
                                if file_name in data_dict:
                                    data_dict[file_name] = pd.concat([data_dict[file_name], df], ignore_index=True)
                                else:
                                    data_dict[file_name] = df      
                        except:
                            continue    
                    df_plant = pd.read_csv(csv_path)  
                    levels = [50,80,100,120,150]
                    for idx, plant_name in enumerate(df_plant["Farm"]):
                        plant_lat = df_plant["latitude"][idx]
                        plant_lon = df_plant["longitude"][idx]
                        plant_height = df_plant["Hhgt"][idx]
                        print("Processing",idx,plant_name,plant_lat,plant_lon,plant_height)
                        plant_data = None
                        for file_name in data_dict.keys():
                            dist = np.sqrt((data_dict[file_name]['lat'] - plant_lat)**2 + (data_dict[file_name]['lon'] - plant_lon)**2)
                            nearest_idx = dist.idxmin()
                            nearest_lat = data_dict[file_name].loc[nearest_idx, 'lat']
                            nearest_lon = data_dict[file_name].loc[nearest_idx, 'lon']
                            nearest_rows = data_dict[file_name][(data_dict[file_name]['lat'] == nearest_lat) & (data_dict[file_name]['lon'] == nearest_lon)]
                            if "temperature" in file_name:
                                nearest_rows.loc[:, 't2m'] = (nearest_rows['t2m'] - 273.15).round(2)
                            if "u_wind" in file_name:
                                df1 = pd.DataFrame()
                                for idx,lev in enumerate(levels):
                                    if idx==0:
                                        df1 = nearest_rows[nearest_rows['lev']==lev].copy()
                                    else:
                                        df1.loc[:, 'u_'+str(lev)] = nearest_rows[nearest_rows['lev']==lev].loc[:, 'u'].values
                                nearest_rows = df1.drop(columns=['lev']).copy()
                                nearest_rows.rename(columns={'u': 'u_50'}, inplace=True)
                            if "v_wind" in file_name:
                                df2 = pd.DataFrame()
                                for idx,lev in enumerate(levels):
                                    if idx==0:
                                        df2 = nearest_rows[nearest_rows['lev']==lev].copy()
                                    else:
                                        df2.loc[:, 'v_'+str(lev)] = nearest_rows[nearest_rows['lev']==lev].loc[:, 'v'].values
                                nearest_rows = df2.drop(columns=['lev']).copy()
                                nearest_rows.rename(columns={'v': 'v_50'}, inplace=True)
                            if plant_data is None:
                                plant_data = nearest_rows
                            else:
                                nearest_rows = nearest_rows.drop(columns=['time','lat','lon'])
                                plant_data = pd.concat([plant_data.reset_index(drop=True), nearest_rows.reset_index(drop=True)], axis=1) 
                        plant_data = plant_data.reset_index(drop=True)
                        plant_data['rh2m'] = compute_specific_humidity(plant_data['t2m'], plant_data['press'], plant_data['rh2m']) 
                        plant_data['rf'] = plant_data['rf'].round(2)
                        for lev in levels:
                            plant_data['WindSpeed_'+str(lev)+'(m/s)'] = np.sqrt(plant_data['u_'+str(lev)]**2 + plant_data['v_'+str(lev)]**2).round(2)
                            plant_data['WindDirection_'+str(lev)+'(deg)'] = (np.degrees(np.arctan2(plant_data['u_'+str(lev)],plant_data['v_'+str(lev)])) + 180) % 360
                            plant_data['WindDirection_'+str(lev)+'(deg)'] = plant_data['WindDirection_'+str(lev)+'(deg)'].round(2)
                            plant_data = plant_data.drop(columns=['u_'+str(lev),'v_'+str(lev)])
                        plant_data.rename(columns={
                        't2m':'Temperature(degC)',    
                        'press': 'Pressure(Pa)',
                        'dswrf': 'GHI(W/m^2)',
                        'rf':'Rainfall(kg/m^2)',
                        'rh2m':'SpecificHumidity(kg/kg)'
                        }, inplace=True)
                        dates_list = sorted(set(pd.to_datetime(plant_data['time'], errors='coerce').dt.date.to_list()))
                        for date in dates_list:
                            df_date_15mins = plant_data[pd.to_datetime(plant_data['time'], errors='coerce').dt.date==date].copy()
                            df_date_15mins['time'] = pd.to_datetime(df_date_15mins['time'], errors='coerce')
                            df_date_15mins.set_index('time', inplace=True)
                            df_date_5mins = df_date_15mins.resample('5min').interpolate(method="linear")
                            table = pa.Table.from_pandas(df_date_15mins)
                            buffer = io.BytesIO()
                            pq.write_table(table, buffer)
                            buffer.seek(0)
                            s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/"+str(cycle)+"_utc"+"/Day_wise_data/15_mins/"+plant_name+"/"+str(date)+".parquet",Body=buffer.getvalue())
                            table = pa.Table.from_pandas(df_date_5mins)
                            buffer = io.BytesIO()
                            pq.write_table(table, buffer)
                            buffer.seek(0)
                            s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/"+str(cycle)+"_utc"+"/Day_wise_data/5_mins/"+plant_name+"/"+str(date)+".parquet",Body=buffer.getvalue())
                        table = pa.Table.from_pandas(plant_data)
                        buffer = io.BytesIO()
                        pq.write_table(table, buffer)
                        buffer.seek(0)
                        s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/"+str(cycle)+"_utc"+"/Plant_wise_data/"+plant_name+".parquet",Body=buffer.getvalue())
                    for file_name in data_dict.keys():
                        table = pa.Table.from_pandas(data_dict[file_name])
                        buffer = io.BytesIO()
                        pq.write_table(table, buffer)
                        buffer.seek(0)
                        s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/"+str(cycle)+"_utc"+"/parquet/"+file_name+".parquet",Body=buffer.getvalue())
                send_mail("Successfully retrieved the data from Weather API for the execution date: " + str(datetime.strptime(str(inputdate), "%Y%m%d").strftime("%Y-%m-%d")) + " " +str(cycle)+"_UTC.")
                return True        
            else:
                send_mail("Weather API is not responding for the execution date: " + str(datetime.strptime(str(inputdate), "%Y%m%d").strftime("%Y-%m-%d")) + " "+str(cycle)+"_UTC.")
                return False

import pytz
from datetime import timedelta
ist = pytz.timezone('Asia/Kolkata')
yesterday = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
inputdate = datetime.now(ist).strftime('%Y%m%d')
prefix ="weather_data/processed"
inputdate =inputdate
cycle = "00"
inputdate = datetime.strptime(inputdate, "%Y%m%d")
formatted_date = inputdate.strftime("%Y-%m-%d")
print(formatted_date,type(formatted_date))
# processing_files(prefix,inputdate,cycle)