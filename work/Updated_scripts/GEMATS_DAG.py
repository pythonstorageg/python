from airflow import DAG
from airflow.sensors.python import PythonSensor
from datetime import datetime
import numpy as np
from weather_data_credentials import config_setup
import requests,pytz,os
import pandas as pd

ist1 = pytz.timezone("Asia/Kolkata")
df_marketarea = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "GEMATS_Market_locations.csv"))

def compute_specific_humidity(temp_c,pressure_pa,rh):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def open_meteo_forecast():
    for idx, id in enumerate(df_marketarea['id']):
        latitude = df_marketarea['Lattitude'][idx]
        longitude = df_marketarea['Longitude'][idx]
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,rain,windspeed_10m,shortwave_radiation,relative_humidity_2m,surface_pressure,cloudcover",
            "forecast_days": 7,
            "timezone": "auto"
        }
        response = requests.get(url, params=params)
        df = pd.DataFrame(response.json()["hourly"])
        df['surface_pressure'] = 100 * df['surface_pressure']
        df['SpecificHumidity'] = compute_specific_humidity(df['temperature_2m'], df['surface_pressure'], df['relative_humidity_2m'])
        for idx1, date in enumerate(df['time']):
            dt = datetime.strptime(date, "%Y-%m-%dT%H:%M")
            year = dt.year
            month = dt.month
            day = dt.day
            weekday = dt.weekday()
            start_time = ist1.localize(dt)
            rain_fall = df['rain'][idx1]
            temp = df['temperature_2m'][idx1]
            radiation = df['shortwave_radiation'][idx1]
            humidity = df['SpecificHumidity'][idx1]
            velocity = df['windspeed_10m'][idx1]
            press = df['surface_pressure'][idx1],
            cloud_cover = df['cloudcover'][idx1],
            db_jj = {"area":id,"start_time":start_time,
                            "temperature":float(temp),"radiation":float(radiation),"specific_humidity":float(humidity),"pressure":float(press[0]),"rain_fall":float(rain_fall),
                            "day":day,"year":year,"month":month,"weekday":weekday,"velocity":float(velocity),"cloud_cover":float(cloud_cover[0])}
            print(db_jj)
            config_setup['gemats_db']['open_meteo_forecast'].update_one({"start_time":start_time,"area":id},{"$set": db_jj},upsert=True)
            print("uploaded to Mongo")
    return True   
with DAG('open-meteo-forecast',start_date=datetime(2025, 5, 1, 0, 0),
    schedule_interval='0 4,10,16,22 * * *', catchup=False) as dag:
    task = PythonSensor(
        task_id="open-meteo-forecast",
        python_callable=open_meteo_forecast,
        poke_interval=3600,  # 1 hr
        timeout=10800,     # 3 hrs  
        mode="poke",       
        soft_fail=False
    )




