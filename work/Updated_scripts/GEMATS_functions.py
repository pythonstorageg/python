import pandas as pd
import numpy as np
import pytz,os,requests,smtplib
from email.mime.text import MIMEText
from datetime import datetime,timedelta
from weather_data_credentials import config_setup
import xarray as xr

ist1 = pytz.timezone("Asia/Kolkata")
df_marketarea = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "GEMATS_Market_locations.csv"))

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

def compute_specific_humidity(temp_c,pressure_pa,rh):
    pressure_hpa = pressure_pa / 100.0
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    e = rh * es / 100.0
    q = (0.622 * e) / (pressure_hpa - (0.378 * e))
    return round(q,2)

def temp_file_extract(temp_file_path,days_number,year):
    data = np.fromfile(temp_file_path, dtype=np.float32).reshape((days_number, 31, 31))
    latitudes = np.arange(7.5, 38.0, 1.0)    
    longitudes = np.arange(67.5, 98.0, 1.0) 
    start_date = datetime(year, 1, 1)        
    records = []
    for day_idx in range(days_number):
        current_date = start_date + timedelta(days=day_idx)
        for i, lat in enumerate(latitudes):
            for j, lon in enumerate(longitudes):
                temp = data[day_idx, i, j]
                records.append((current_date.strftime("%Y-%m-%d"), lat, lon, temp))
    df_temp = pd.DataFrame(records, columns=["date", "lat", "lon", "temperature"])
    return df_temp

def parameter_nearest_rows(df,state_lat,state_lon):
    dist = np.sqrt((df['lat'] - state_lat)**2 + (df['lon'] - state_lon)**2)
    nearest_idx = dist.idxmin()
    nearest_lat = df.loc[nearest_idx, 'lat']
    nearest_lon = df.loc[nearest_idx, 'lon']
    nearest_rows = df[(df['lat'] == nearest_lat) & (df['lon'] == nearest_lon)].copy()
    nearest_rows= nearest_rows.reset_index(drop=True)
    return nearest_rows

def open_meteo_historical():
    for idx, id in enumerate(df_marketarea['id']):
        latitude = df_marketarea['Lattitude'][idx]
        longitude = df_marketarea['Longitude'][idx]
        start_date = "2020-01-01"
        end_date = "2020-12-31"
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,rain_sum",
            "timezone": "auto"
        }
        response = requests.get(url, params=params)
        df = pd.DataFrame(response.json()["daily"])
        for idx1, date in enumerate(df['time']):
            dt = datetime.strptime(date, "%Y-%m-%d")
            year = dt.year
            month = dt.month
            day = dt.day
            weekday = dt.weekday()
            date = ist1.localize(dt)
            max_temp = df['temperature_2m_max'][idx1]
            min_temp = df['temperature_2m_min'][idx1]
            rain_fall = df["rain_sum"][idx1]
            db_jj = {"area":id,"year":year,"month":month,"day":day,"week_day":weekday,"max_temperature":float(max_temp),
                            "min_temperature":float(min_temp),"rain_fall":float(rain_fall),"date":date}
            # config_setup['gemats_db']['open_meteo_historical'].insert_one(db_jj)
            
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
        print(df)
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
        #     config_setup['gemats_db']['open_meteo_forecast'].update_one({"start_time":start_time,"area":id},{"$set": db_jj},upsert=True)

def IMD_historical(max_temp_path,min_temp_path,rainfall_path,year):
    if year%4 == 0:
        days_number = 366
    else:
        days_number = 365  
    df_max_temp = temp_file_extract(max_temp_path,days_number,year)
    df_min_temp = temp_file_extract(min_temp_path,days_number,year) 
    ds = xr.open_dataset(rainfall_path)
    df_rain_fall = ds.to_dataframe().reset_index()
    try:
        df_rain_fall.rename(columns={
                            'RAINFALL':'rainfall',
                            "LATITUDE":"lat",
                            "LONGITUDE":'lon'
                            }, inplace=True)
    except:
        print("Column names already in required format")
    for idx, id in enumerate(df_marketarea['Code']):
        mkt_lat = df_marketarea['Lattitude'][idx]
        mkt_lon = df_marketarea['Longitude'][idx]
        nearest_rows_maxT = parameter_nearest_rows(df_max_temp,mkt_lat,mkt_lon)
        nearest_rows_maxT.rename(columns={'temperature':'temperature_max'}, inplace=True)
        nearest_rows_minT = parameter_nearest_rows(df_min_temp,mkt_lat,mkt_lon)
        nearest_rows_minT.rename(columns={'temperature':'temperature_min'}, inplace=True)
        nearest_rows_rf = parameter_nearest_rows(df_rain_fall,mkt_lat,mkt_lon)
        nearest_rows_rf.rename(columns={'rf':'rainfall'}, inplace=True)
        df = pd.concat([nearest_rows_maxT['date'],nearest_rows_maxT['temperature_max'],nearest_rows_minT['temperature_min'],nearest_rows_rf['rainfall']],axis=1) 
        for idx1, date in enumerate(df['date']):
            dt = datetime.strptime(date, "%Y-%m-%d")
            year = dt.year
            month = dt.month
            day = dt.day
            weekday = dt.weekday()
            date = ist1.localize(dt)
            max_temp = df['temperature_max'][idx1]
            min_temp = df['temperature_min'][idx1]
            rain_fall = df["rainfall"][idx1]
            db_jj = {"area":id,"year":year,"month":month,"day":day,"week_day":weekday,"max_temperature":float(max_temp),
                            "min_temperature":float(min_temp),"rain_fall":float(rain_fall),"date":date}
            # config_setup['gemats_db']['IMD_market_wise'].insert_one(db_jj)


# open_meteo_forecast()
# open_meteo_historical()
# IMD_historical("/home/gopi/doker_airflow/IMD/2024/Maxtemp_MaxT_2024.GRD",
#                  "/home/gopi/doker_airflow/IMD/2024/Mintemp_MinT_2024.GRD",
#                  "/home/gopi/doker_airflow/IMD/2024/RF25_ind2024_rfp25.nc",2024)
        




