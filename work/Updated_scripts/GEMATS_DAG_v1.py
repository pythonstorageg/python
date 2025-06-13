# from airflow import DAG
# from airflow.sensors.python import PythonSensor
from datetime import datetime as dt
import numpy as np
import requests,pytz,os,yaml,boto3,io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)),".env"))

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)
bucket_name = os.getenv("MINIO_SOURCE_BUCKET")

db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))  
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
client = MongoClient(f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/?authMechanism=SCRAM-SHA-256&authSource=geeml")
# client = MongoClient(f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/?authSource=admin&authMechanism=SCRAM-SHA-256")
# client = MongoClient(f"mongodb://{db_host}:{db_port}")
db_iex = client[os.getenv("DB_NAME_DEF")]
db_def = client[os.getenv("DB_NAME_DEF")]
collection_geo_locations = db_def['GEO_LOCATION']
collection_meteo_forecast = db_iex['open_meteo_forecast'] 
collection_meteo_api_interaction = db_iex['open_meteo_api_interaction'] 
collection_definition = db_iex['DEFINITIONS']

ist = pytz.timezone("Asia/Kolkata")
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.yaml"), "r") as file:
    config = yaml.safe_load(file)

def compute_specific_humidity(temp_c,pressure_pa,rh):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def api_interaction_status(start_time,status,msg):
    end_time = dt.now(ist).replace(tzinfo=None)
    collection_meteo_api_interaction.insert_one({"year":start_time.year,"month":start_time.month,"day":start_time.day,
                                                                     "week_day":start_time.weekday(),"start_time":ist.localize(start_time),"end_time":ist.localize(end_time),"status":status,
                                                                     "message":msg})
def open_meteo_forecast():
    start_time = dt.now(ist).replace(tzinfo=None)
    mkt_geo_locations = []
    try:
        geo_location_data = collection_geo_locations.find({"TYPE":{"$in":config['gemats_db']['definition_geo_location_key_lst']}},{"CODE":1,"LATITUDE":1,"LONGITUDE":1}).to_list()
        definitions_data = collection_definition.find({"DOC_TYPE":{"$in":config['gemats_db']['iex_definitions_key_lst']}},{"CODE":1,"LOC":1}).to_list()
        for i in definitions_data:
            for j in geo_location_data:
                if i['LOC'] == j["_id"]:
                    mkt_geo_locations.append([i['CODE'],j['LATITUDE'],j['LONGITUDE'],i['_id']])
                    print(i['CODE'],j['LATITUDE'],j['LONGITUDE'],i['_id'])
    except Exception as e:
        print("error",e)
        api_interaction_status(start_time,False,"unable to update the weather data:"+str(e))
        return False
    print(mkt_geo_locations)

    for code,lat, lon, id in mkt_geo_locations:
        url = config['open_meteo']['forecast_url']
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": config['open_meteo']['hourly_parameters_lst'],
            "forecast_days": 7,
            "timezone": "auto"
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            try:
                print("response",response)
                df = pd.DataFrame(response.json()["hourly"])
                df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M")
                df['year'] = df['time'].dt.year
                df['month'] = df['time'].dt.month
                df['day'] = df['time'].dt.day
                df['week_day'] = df['time'].dt.weekday
                df['start_time'] = df['time'].dt.tz_localize(ist).dt.tz_convert('UTC')
                df['area'] = str(id)
                df = df.rename(columns=dict(zip(config['open_meteo']['hourly_parameters_lst'],config['open_meteo']['renamed_parameters_lst'])))
                df[config['open_meteo']['renamed_parameters_lst']] = df[config['open_meteo']['renamed_parameters_lst']].astype(float)
                df['pressure'] = 100 * df['pressure']
                df['specific_humidity'] = compute_specific_humidity(df['temperature'], df['pressure'], df['relative_humidity']).astype(float)
                df.drop(columns=['time'],inplace=True)
                for record in df.to_dict("records"):
                    collection_meteo_forecast.update_one({"start_time": record["start_time"],"area": record["area"]},{"$set": record},upsert=True)  
                table = pa.Table.from_pandas(df)
                buffer = io.BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)
                s3.put_object(Bucket=bucket_name,Key="GEMATS/open_meteo_forecast/"+str(start_time.strftime("%Y-%m-%d/%H:%M"))+"/"+str(code)+".parquet",Body=buffer.getvalue())    
                print("uploaded to MinIO")
            except Exception as e:
                api_interaction_status(start_time,False,"unable to update the weather data:"+str(e))
                return False
        else:
            api_interaction_status(start_time,False,"open-meteo api is not responding")
            return False
    api_interaction_status(start_time,True,"Successfully updated the weather data")
    return True   
open_meteo_forecast()
# with DAG('open-meteo-forecast',start_date=dt(2025, 5, 1, 0, 0),
#     schedule_interval='0 4,10,16,22 * * *', catchup=False) as dag:
#     task = PythonSensor(
#         task_id="open-meteo-forecast",
#         python_callable=open_meteo_forecast,
#         poke_interval=3600,  # 1 hr
#         timeout=10800,     # 3 hrs  
#         mode="poke",       
#         soft_fail=False
#     )



