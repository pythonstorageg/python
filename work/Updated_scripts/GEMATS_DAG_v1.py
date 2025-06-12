# from airflow import DAG
# from airflow.sensors.python import PythonSensor
from datetime import datetime as dt
import numpy as np
from weather_data_credentials import config_setup
import requests,pytz,os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))  
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
client = MongoClient(f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/?authSource=admin&authMechanism=SCRAM-SHA-256")
db_iex = client[os.getenv("DB_NAME_IX")]
db_def = client[os.getenv("DB_NAME_DEF")]
collection_meteo_forecast = db_iex[os.getenv('OPENMETEOFORECAST_COLLECTION')] 
collection_geo_locations = db_def[os.getenv('GEOLOCATIONS_COLLECTION')]
print(collection_geo_locations)
collection_meteo_api_interaction = db_iex[os.getenv('OPENMETEOAPI_COLLECTION')] 
collection_definition = db_iex[os.getenv('DEFINITION_COLLECTION')]

ist = pytz.timezone("Asia/Kolkata")
df_marketarea = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "GEMATS_Market_locations.csv"))

def compute_specific_humidity(temp_c,pressure_pa,rh):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def api_interaction_status(start_time,status,msg):
    end_time = dt.strptime(str(dt.now()), "%Y-%m-%d %H:%M:%S.%f") 
    config_setup['gemats_db']['open_meteo_api_interactions'].insert({"year":start_time.year,"month":start_time.month,"day":start_time.day,
                                                                     "week_day":start_time.weekday(),"start_time":start_time,"end_time":end_time,"status":status,
                                                                     "message":msg})

def open_meteo_forecast():
    start_time = dt.strptime(str(dt.now()), "%Y-%m-%d %H:%M:%S.%f") 
    mkt_geo_locations = []
    try:
        geo_location_data = collection_geo_locations.find({"TYPE":{"$in":["RLDC","MARKET_AREA","COUNTRY"]}},{"CODE":1,"LATITUDE":1,"LONGITUDE":1}).to_list()
        print(geo_location_data)
        definitions_data = collection_definition.find({"DOC_TYPE":"MARKET AREA DEFINITION"},{"CODE":1,"LOC":1}).to_list()
        print(definitions_data)
        for i in definitions_data:
            for j in geo_location_data:
                if i['LOC'] == j["_id"]:
                    mkt_geo_locations.append([j['LATITUDE'],j['LONGITUDE'],i['_id']])
                    print(i['CODE'],j['LATITUDE'],j['LONGITUDE'],i['_id'])
    except Exception as e:
        print("error",e)
        # api_interaction_status(start_time,False,"unable to update the weather data:"+str(e))
        return False
    print(mkt_geo_locations)

    # for lat, lon, id in mkt_geo_locations:
    #     url = "https://api.open-meteo.com/v1/forecast"
    #     params = {
    #         "latitude": lat,
    #         "longitude": lon,
    #         "hourly": "temperature_2m,rain,windspeed_10m,shortwave_radiation,relative_humidity_2m,surface_pressure,cloudcover",
    #         "forecast_days": 7,
    #         "timezone": "auto"
    #     }
    #     response = requests.get(url, params=params)
    #     if response.status_code == 200:
    #         try:
    #             print("response",response)
    #             df = pd.DataFrame(response.json()["hourly"])
    #             df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%dT%H:%M")
    #             df['year'] = df['time'].dt.year
    #             df['month'] = df['time'].dt.month
    #             df['day'] = df['time'].dt.day
    #             df['week_day'] = df['time'].dt.weekday
    #             df['start_time'] = df['time'].dt.tz_localize(ist).dt.tz_convert('UTC')
    #             df['area'] = str(id)
    #             df = df.rename(columns=dict(zip(['temperature_2m','shortwave_radiation','rain','cloudcover','windspeed_10m','surface_pressure',"relative_humidity_2m","winddirection"], 
    #                                             ['temperature','radiation','rain_fall','cloud_cover','velocity','pressure',"relative_humidity","wind_direction"])))
    #             cols_to_convert = ['temperature','radiation','rain_fall','cloud_cover','velocity','pressure','relative_humidity_2m',"winddirection"]
    #             df[cols_to_convert] = df[cols_to_convert].astype(float)
    #             df['pressure'] = 100 * df['pressure']
    #             df['specific_humidity'] = compute_specific_humidity(df['temperature'], df['pressure'], df['relative_humidity_2m']).astype(float)
    #             df.drop(columns=['time'],inplace=True)
    #             for record in df.to_dict("records"):
    #                 config_setup['gemats_db']['open_meteo_forecast'].update_one({"start_time": record["start_time"],"area": record["area"]},{"$set": record},upsert=True)  
    #         except Exception as e:
    #             api_interaction_status(start_time,False,"unable to update the weather data:"+str(e))
    #             return False
    #     else:
    #         api_interaction_status(start_time,False,"open-meteo api is not responding")
    #         return False
    # api_interaction_status(start_time,True,"Successfully updated the weather data")
    # return True   

# with DAG('open-meteo-forecast',start_date=datetime(2025, 5, 1, 0, 0),
#     schedule_interval='0 4,10,16,22 * * *', catchup=False) as dag:
#     task = PythonSensor(
#         task_id="open-meteo-forecast",
#         python_callable=open_meteo_forecast,
#         poke_interval=3600,  # 1 hr
#         timeout=10800,     # 3 hrs  
#         mode="poke",       
#         soft_fail=False
#     )

open_meteo_forecast()


