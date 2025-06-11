# from airflow import DAG
# from airflow.sensors.python import PythonSensor
from datetime import datetime as dt
import numpy as np
from weather_data_credentials import config_setup
import requests,pytz,os
import pandas as pd

ist = pytz.timezone("Asia/Kolkata")
df_marketarea = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "GEMATS_Market_locations.csv"))

def compute_specific_humidity(temp_c,pressure_pa,rh):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def open_meteo_forecast():
    for id_, lat, lon in zip(df_marketarea['id'], df_marketarea['Lattitude'], df_marketarea['Longitude']):
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,rain,windspeed_10m,shortwave_radiation,relative_humidity_2m,surface_pressure,cloudcover",
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
                df = df.rename(columns=dict(zip(['temperature_2m','shortwave_radiation','rain','cloudcover','windspeed_10m','surface_pressure'], 
                                                ['temperature','radiation','rain_fall','cloud_cover','velocity','pressure'])))
                cols_to_convert = ['temperature','radiation','rain_fall','cloud_cover','velocity','pressure','relative_humidity_2m']
                df[cols_to_convert] = df[cols_to_convert].astype(float)
                df['pressure'] = 100 * df['pressure']
                df['specific_humidity'] = compute_specific_humidity(df['temperature'], df['pressure'], df['relative_humidity_2m']).astype(float)
                df.drop(columns=['relative_humidity_2m','time'],inplace=True)
                print(df.keys())
                for record in df.to_dict("records"):
                    config_setup['gemats_db']['open_meteo_forecast'].update_one({"start_time": record["start_time"],"area": record["area"]},{"$set": record},upsert=True)  
            except Exception as e:
                print(e)
                return False
        else:
            return False
    return True   
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


