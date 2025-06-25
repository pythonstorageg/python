from sqlalchemy import create_engine,text
import sqlalchemy,os
import pandas as pd
import numpy as np
import pandas as pd
import pytz,yaml
import statsmodels.api as sm
import matplotlib.pyplot as plt
from pymongo import MongoClient
# from airflow import DAG
from datetime import datetime as dt
# from airflow.sensors.python import PythonSensor

ist = pytz.timezone('Asia/Kolkata')

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.yaml"), "r") as file:
    config = yaml.safe_load(file)
db_host = config['db_cred']['host']
db_port = config['db_cred']['port'] 
db_user = config['db_cred']['user']
db_password = config['db_cred']['password']
client = MongoClient(f"mongodb://geeml:TdlTgEFHsYSQSDo@10.88.0.201:5000/?authMechanism=SCRAM-SHA-256&authSource=geeml")
print("connected")
db = client[config['db_cred']['DEF_DB']]
coll = db['open_meteo_collection']
def getting_data_from_DB(engine,table_name,start_time):
    query = text(f"""
    SELECT
    (
        TIMESTAMP 'epoch' + FLOOR(EXTRACT(EPOCH FROM "TIMESTAMP") / 900) * 900 * INTERVAL '1 second'
    ) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata' AS "TIMESTAMP_IST",

    -- Upper/Lower Reservoir
    AVG(CASE WHEN "UNIT" = 'UpperReservoir' THEN "MONITOR1" END) AS "Upper_MONITOR1",
    AVG(CASE WHEN "UNIT" = 'UpperReservoir' THEN "MONITOR2" END) AS "Upper_MONITOR2",
    AVG(CASE WHEN "UNIT" = 'LowerReservoir' THEN "MONITOR1" END) AS "Lower_MONITOR1",
    AVG(CASE WHEN "UNIT" = 'LowerReservoir' THEN "MONITOR2" END) AS "Lower_MONITOR2",

    -- UNIT1 to UNIT8
    AVG(CASE WHEN "UNIT" = 'UNIT1' THEN "MW" END) AS "UNIT_1_P",
    AVG(CASE WHEN "UNIT" = 'UNIT2' THEN "MW" END) AS "UNIT_2_P",
    AVG(CASE WHEN "UNIT" = 'UNIT3' THEN "MW" END) AS "UNIT_3_P",
    AVG(CASE WHEN "UNIT" = 'UNIT4' THEN "MW" END) AS "UNIT_4_P",
    AVG(CASE WHEN "UNIT" = 'UNIT5' THEN "MW" END) AS "UNIT_5_P",
    AVG(CASE WHEN "UNIT" = 'UNIT6' THEN "MW" END) AS "UNIT_6_P",
    AVG(CASE WHEN "UNIT" = 'UNIT7' THEN "MW" END) AS "UNIT_7_P",
    AVG(CASE WHEN "UNIT" = 'UNIT8' THEN "MW" END) AS "UNIT_8_P"

    FROM
    {table_name}
    WHERE
    "UNIT" IN (
        'UpperReservoir', 'LowerReservoir',
        'UNIT1', 'UNIT2', 'UNIT3', 'UNIT4',
        'UNIT5', 'UNIT6', 'UNIT7', 'UNIT8'
    )
    AND "TIMESTAMP" >= TIMESTAMPTZ '{start_time}'
    AND "TIMESTAMP" <= (
        SELECT MAX("TIMESTAMP") FROM {table_name}
    )

    GROUP BY
    (
        TIMESTAMP 'epoch' + FLOOR(EXTRACT(EPOCH FROM "TIMESTAMP") / 900) * 900 * INTERVAL '1 second'
    ) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'

    ORDER BY
    "TIMESTAMP_IST" ASC;
    """)

    with engine.connect() as conn:
        df_db = pd.read_sql(query, conn)

    df_db['UpperReservoir'] = df_db.apply(
        lambda r: (r['Upper_MONITOR1'] + r['Upper_MONITOR2']) / 2 if r['Upper_MONITOR1'] > 440 and r['Upper_MONITOR2'] > 440
        else max(r['Upper_MONITOR1'], r['Upper_MONITOR2']) if r['Upper_MONITOR1'] > 440 or r['Upper_MONITOR2'] > 440
        else np.nan,
        axis=1
    )
    df_db['LowerReservoir'] = df_db.apply(
        lambda r: (r['Lower_MONITOR1'] + r['Lower_MONITOR2']) / 2 if r['Lower_MONITOR1'] > 100 and r['Lower_MONITOR2'] > 100
        else max(r['Lower_MONITOR1'], r['Lower_MONITOR2']) if r['Lower_MONITOR1'] > 100 or r['Lower_MONITOR2'] > 100
        else np.nan,
        axis=1
    )
    df_db['UpperReservoir'] = df_db['UpperReservoir'].interpolate()
    df_db['LowerReservoir'] = df_db['LowerReservoir'].interpolate()
    df_db['TIMESTAMP_IST'] = pd.to_datetime(df_db['TIMESTAMP_IST']).dt.tz_localize(ist)
    df_db['TIMESTAMP_IST'] = df_db['TIMESTAMP_IST'].dt.strftime('%Y-%m-%d %H:%M:%S')    
    # df_db['TIMESTAMP_IST'] = df_db['TIMESTAMP_IST'].dt.strftime('%y-%m-%d %H:%M:%S')
    # df_db['TIMESTAMP_IST'] = df_db['TIMESTAMP_IST'].dt.strftime('%d/%m/%y %H:%M:%S')
    df_db = df_db.rename(columns={'TIMESTAMP_IST': 'DateTime_IntBegin','UpperReservoir':"UP_RES_L",'LowerReservoir':"LO_RES_L"})
    df_db = df_db[['DateTime_IntBegin','UNIT_1_P','UNIT_2_P','UNIT_3_P','UNIT_4_P','UNIT_5_P','UNIT_6_P','UNIT_7_P','UNIT_8_P',
            'UP_RES_L','LO_RES_L']]
    return df_db

def calculate_gen_pump_modes(excel_path,turbine_sheet_name,pump_sheet_name):
    gen_mode = pd.read_excel(excel_path,sheet_name=turbine_sheet_name, header=None)
    gen_mode = gen_mode.set_index(gen_mode.columns[0]).T
    gen_mode.columns.name = None
    gen_mode = gen_mode.dropna(axis=1, how='all')
    gen_mode.rename(columns={"Net Head [m]": "G_Net_Head_m", "Discharge [m³/s]": "G_Disch"}, inplace=True)
    gen_mode["G_Disch"] = -1 * gen_mode["G_Disch"]
    gen_mode = gen_mode[["G_Net_Head_m", "G_Disch"]]
    gen_mode.sort_values("G_Net_Head_m", ascending=True, inplace=True)

    pump_mode = pd.read_excel(excel_path,sheet_name=pump_sheet_name, header=None)
    pump_mode = pump_mode.set_index(pump_mode.columns[0]).T
    gen_mode.columns.name = None
    gen_mode = gen_mode.dropna(axis=1, how='all')
    pump_mode.rename(columns={"Net Head [m]": "P_Net_Head_m", "Discharge [m³/s]": "P_Disch"}, inplace=True)
    pump_mode = pump_mode[["P_Net_Head_m", "P_Disch"]]
    pump_mode.sort_values("P_Net_Head_m", ascending=True, inplace=True)
    return gen_mode,pump_mode

def comb_psp_data(PSP_Data,gen_mode,pump_mode):
    U1_s = [
        (PSP_Data["UNIT_1_P"] < -100),
        (PSP_Data["UNIT_1_P"] > 100),
        (PSP_Data["UNIT_1_P"] >= -100) & (PSP_Data["UNIT_1_P"] <= 100)  # Normal
    ]
    # define results
    U1_s_r = [-1, 1, 0]
    PSP_Data["U1_Stat"] = np.select(U1_s, U1_s_r, default=0)
    U2_s = [
        (PSP_Data["UNIT_2_P"] < -100),
        (PSP_Data["UNIT_2_P"] > 100),
        (PSP_Data["UNIT_2_P"] >= -100) & (PSP_Data["UNIT_2_P"] <= 100)  # Normal
    ]
    # define results
    U2_s_r = [-1, 1, 0]
    PSP_Data["U2_Stat"] = np.select(U2_s, U2_s_r, default=0)
    U3_s = [
        (PSP_Data["UNIT_3_P"] < -100),
        (PSP_Data["UNIT_3_P"] > 100),
        (PSP_Data["UNIT_3_P"] >= -100) & (PSP_Data["UNIT_3_P"] <= 100)  # Normal
    ]
    # define results
    U3_s_r = [-1, 1, 0]
    PSP_Data["U3_Stat"] = np.select(U3_s, U3_s_r, default=0)
    U4_s = [
        (PSP_Data["UNIT_4_P"] < -100),
        (PSP_Data["UNIT_4_P"] > 100),
        (PSP_Data["UNIT_4_P"] >= -100) & (PSP_Data["UNIT_4_P"] <= 100)  # Normal
    ]
    # define results
    U4_s_r = [-1, 1, 0]
    PSP_Data["U4_Stat"] = np.select(U4_s, U4_s_r, default=0)
    U5_s = [
        (PSP_Data["UNIT_5_P"] < -100),
        (PSP_Data["UNIT_5_P"] > 100),
        (PSP_Data["UNIT_5_P"] >= -100) & (PSP_Data["UNIT_5_P"] <= 100)  # Normal
    ]
    # define results
    U5_s_r = [-1, 1, 0]
    PSP_Data["U5_Stat"] = np.select(U5_s, U5_s_r, default=0)
    U6_s = [
        (PSP_Data["UNIT_6_P"] < -100),
        (PSP_Data["UNIT_6_P"] > 100),
        (PSP_Data["UNIT_6_P"] >= -100) & (PSP_Data["UNIT_6_P"] <= 100)  # Normal
    ]
    # define results
    U6_s_r = [-1, 1, 0]
    PSP_Data["U6_Stat"] = np.select(U6_s, U6_s_r, default=0)

    U7_s = [
        (PSP_Data["UNIT_7_P"] < -100),
        (PSP_Data["UNIT_7_P"] > 100),
        (PSP_Data["UNIT_7_P"] >= -100) & (PSP_Data["UNIT_7_P"] <= 100)  # Normal
    ]
    # define results
    U7_s_r = [-1, 1, 0]
    PSP_Data["U7_Stat"] = np.select(U7_s, U7_s_r, default=0)
    U8_s = [
        (PSP_Data["UNIT_8_P"] < -100),
        (PSP_Data["UNIT_8_P"] > 100),
        (PSP_Data["UNIT_8_P"] >= -100) & (PSP_Data["UNIT_8_P"] <= 100)  # Normal
    ]
    # define results
    U8_s_r = [-1, 1, 0]
    PSP_Data["U8_Stat"] = np.select(U8_s, U8_s_r, default=0)

    PSP_Data["ON_Units"] = PSP_Data["U1_Stat"].fillna(0) + PSP_Data["U2_Stat"].fillna(0) + PSP_Data["U3_Stat"].fillna(0) + \
                        PSP_Data["U4_Stat"].fillna(0) + PSP_Data["U5_Stat"].fillna(0) + PSP_Data["U6_Stat"].fillna(0) + \
                        PSP_Data[
                            "U7_Stat"].fillna(0) + PSP_Data["U8_Stat"].fillna(0)
    PSP_Data["P_Total"] = PSP_Data["UNIT_1_P"].fillna(0) + PSP_Data["UNIT_2_P"].fillna(0) + PSP_Data["UNIT_3_P"].fillna(0) + \
                        PSP_Data["UNIT_4_P"].fillna(0) + PSP_Data["UNIT_5_P"].fillna(0) + PSP_Data[
                            "UNIT_6_P"].fillna(0) + PSP_Data["UNIT_7_P"].fillna(0) + PSP_Data["UNIT_8_P"].fillna(0)
    PSP_Data = PSP_Data[(abs(PSP_Data['P_Total']) >= 200) & (PSP_Data['UP_RES_L'].notna())].copy()
    PSP_Data = PSP_Data.reset_index(drop=True)
    PSP_Data["P_N"] = PSP_Data["P_Total"].apply(lambda x: x / 245 if x > 0 else x / 256)
    PSP_Data["Net_Head_m"] = PSP_Data["UP_RES_L"] - PSP_Data["LO_RES_L"] - 2.5
    PSP_Data.sort_values(by="Net_Head_m", inplace=True)
    PSP_Data['Net_Head_m'] = pd.to_numeric(PSP_Data['Net_Head_m'], errors='coerce')
    gen_mode['G_Net_Head_m'] = pd.to_numeric(gen_mode['G_Net_Head_m'], errors='coerce')
    PSP_Data = PSP_Data.dropna(subset=["Net_Head_m"])
    gen_mode = gen_mode.dropna(subset=["G_Net_Head_m"]) 
    comb_psp = pd.merge_asof(PSP_Data, gen_mode, left_on="Net_Head_m", right_on="G_Net_Head_m")
    comb_psp = pd.merge_asof(comb_psp, pump_mode, left_on="Net_Head_m", right_on="P_Net_Head_m")
    comb_psp = comb_psp[comb_psp.ON_Units != 0]
    comb_psp['DateTime_IntBegin'] = pd.to_datetime(comb_psp['DateTime_IntBegin'], errors='coerce')
    comb_psp.sort_values(by="DateTime_IntBegin", axis=0, ascending=True, inplace=True, ignore_index=True)
    comb_psp["Delta_V_mcm"] = comb_psp.apply(
        lambda row: row['G_Disch'] * row['P_N'] if row['P_N'] > 0 else -1 * row['P_Disch'] * row['P_N'],
        axis=1) * 15 * 60 / (10e5)
    comb_psp["Delta_URL"] = comb_psp["UP_RES_L"].diff()
    comb_psp["Machine_Hrs_Real"] = 0.25 * comb_psp["P_N"]

    comb_psp['time_diff'] = comb_psp['DateTime_IntBegin'].diff()
    comb_psp = comb_psp[(comb_psp['time_diff'] <= pd.Timedelta('15min')) | (comb_psp['time_diff'].isna())]
    df = comb_psp.copy()
    same_sign_mask = ((df['Delta_URL'] > 0) & (df['P_Total'] > 0)) | ((df['Delta_URL'] < 0) & (df['P_Total'] < 0))
    comb_psp = comb_psp[~same_sign_mask]  
    comb_psp.index = comb_psp["DateTime_IntBegin"]
    comb_psp.drop(columns=["DateTime_IntBegin", "U1_Stat", "U2_Stat", "U3_Stat", "U4_Stat", "U5_Stat", "U6_Stat", "U7_Stat",
                        "U8_Stat",'time_diff'], inplace=True)
    comb_psp.to_csv("/home/gopi/doker_airflow/dags/test.csv")
    return comb_psp

def plotting_xy_scatter(df,x,y):
    plt.scatter(df[x], df[y])
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f'Scatter Plot of {x} vs {y}')
    plt.grid(True)
    plt.savefig('scatter_plot1.jpg', format='jpg', dpi=300)

def getting_data_from_db(engine,table):
    query = text(f"""
    SELECT * FROM "{table}" """)
    with engine.connect() as conn:
        df_db = pd.read_sql(query, conn)
    df_db['UP_RES_L'] = df_db.apply(
    lambda r: (r['Upper_MONITOR1'] + r['Upper_MONITOR2']) / 2 if r['Upper_MONITOR1'] > 440 and r['Upper_MONITOR2'] > 440
    else max(r['Upper_MONITOR1'], r['Upper_MONITOR2']) if r['Upper_MONITOR1'] > 440 or r['Upper_MONITOR2'] > 440
    else np.nan,
    axis=1
    )
    df_db['LO_RES_L'] = df_db.apply(
        lambda r: (r['Lower_MONITOR1'] + r['Lower_MONITOR2']) / 2 if r['Lower_MONITOR1'] > 100 and r['Lower_MONITOR2'] > 100
        else max(r['Lower_MONITOR1'], r['Lower_MONITOR2']) if r['Lower_MONITOR1'] > 100 or r['Lower_MONITOR2'] > 100
        else np.nan,
        axis=1
    )
    df_db.drop(columns=['Unnamed: 0','Upper_MONITOR1','Upper_MONITOR2','Lower_MONITOR1','Lower_MONITOR2'],inplace=True)
    df_db.rename(columns={"TIMESTAMP_IST":"DateTime_IntBegin"},inplace=True)
    df_db["DateTime_IntBegin"] = pd.to_datetime(df_db["DateTime_IntBegin"],utc=True)
    df_db["DateTime_IntBegin"] = df_db["DateTime_IntBegin"].dt.tz_convert('Asia/Kolkata')
    df_db["DateTime_IntBegin"] = df_db["DateTime_IntBegin"].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df_db

def processing():
    try:
        start_time = dt.now(ist).replace(tzinfo=None)
        engine = create_engine(config['time_scale']['url'])
        df_db = getting_data_from_DB(engine,config['time_scale']['source_table'],config['time_scale']['start_date'])

        # engine = create_engine("postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database")
        # df_db = getting_data_from_db(engine,"PSP_water")

        gen_mode,pump_mode = calculate_gen_pump_modes(os.path.join(os.path.dirname(os.path.abspath(__file__)),"Pinnapuram_PT126_PT208_LARGE_SMALL_Export_Pmax_PU_TU.xlsx"),
                                                    "PINNA_LARGE_TU","PINNA_LARGE_PU")
        df_old = pd.read_excel(os.path.join(os.path.dirname(os.path.abspath(__file__)),"comb_psp.xlsx"),
                                sheet_name="comb_psp", header=0)
        df_old = df_old[['DateTime_IntBegin','UNIT_1_P','UNIT_2_P','UNIT_3_P','UNIT_4_P','UNIT_5_P','UNIT_6_P','UNIT_7_P','UNIT_8_P',
                'UP_RES_L','LO_RES_L']]
        PSP_Data = pd.concat([df_old, df_db], ignore_index=True)
        PSP_Data['DateTime_IntBegin'] = pd.to_datetime(PSP_Data['DateTime_IntBegin'], errors='coerce')
        PSP_Data = PSP_Data.drop_duplicates(subset='DateTime_IntBegin', keep='first')
        comb_psp_df = comb_psp_data(PSP_Data,gen_mode,pump_mode)
        comb_psp_df["Delta_URL"]=comb_psp_df["Delta_URL"].bfill()
        Q1 = comb_psp_df["Delta_URL"].quantile(0.05)
        Q3 = comb_psp_df["Delta_URL"].quantile(0.95)
        outliers = comb_psp_df[(comb_psp_df["Delta_URL"] < Q1) | (comb_psp_df["Delta_URL"] > Q3)]
        comb_psp_df = comb_psp_df.drop(outliers.index)
        print(comb_psp_df.describe().to_string())
        X=comb_psp_df["Delta_URL"]
        X = sm.add_constant(X)
        reg = sm.OLS(comb_psp_df["Machine_Hrs_Real"],X,hasconst=True)
        reg_f = reg.fit()
        print(reg_f.params)
        m= reg_f.params
        print(m)
        db_jj = {"effective_start_time":ist.localize(start_time),"effective_end_time":ist.localize(pd.Timestamp("2100-01-01")),"const":m['const'],"delta_url":m['Delta_URL'],"psp_code":"ap01"}
        coll.insert_one(db_jj)

        plotting_xy_scatter(comb_psp_df,'Delta_URL','Machine_Hrs_Real')

        comb_psp_df.reset_index(inplace=True)
        comb_psp_df['DateTime_IntBegin'] = pd.to_datetime(
        comb_psp_df['DateTime_IntBegin'], 
        format='%d/%m/%Y %H:%M:%S',  # adjust format if needed
        errors='coerce'
        ).dt.tz_localize('Asia/Kolkata')
        
        comb_psp_df = comb_psp_df[~comb_psp_df.duplicated(subset='DateTime_IntBegin', keep='last')]
        comb_psp_df.to_csv("/home/gopi/doker_airflow/dags/PSP_data.csv")
        comb_psp_df.to_sql(
        name=config['time_scale']['sink_table'],
        con=engine,
        if_exists='replace',  # or 'append'
        index=False,
        dtype={
            'DateTime_IntBegin': sqlalchemy.types.TIMESTAMP(timezone=True)  # for TIMESTAMPTZ
        }
        ) 
        return True
        
    except:
        return False 




        # comb_psp_df = comb_psp_data(PSP_Data,gen_mode,pump_mode)
        # comb_psp_df["Delta_URL"]=comb_psp_df["Delta_URL"].bfill()
        # Q1 = comb_psp_df["Delta_URL"].quantile(0.05)
        # Q3 = comb_psp_df["Delta_URL"].quantile(0.95)
        # outliers = comb_psp_df[(comb_psp_df["Delta_URL"] < Q1) | (comb_psp_df["Delta_URL"] > Q3)]
        # comb_psp_df = comb_psp_df.drop(outliers.index)
        # print(comb_psp_df.describe().to_string())
        # X=comb_psp_df["Delta_URL"]
        # X = sm.add_constant(X)
        # reg = sm.OLS(comb_psp_df["Machine_Hrs_Real"],X,hasconst=True)
        # reg_f = reg.fit()
        # print(reg_f.params)
        # m= reg_f.params
        # print(m)
        # print(type(m),m['const'],m['Delta_URL'])
        # db_jj = {"effective_start_time":ist.localize(start_time),"effective_end_time":ist.localize(pd.Timestamp("2100-01-01")),"const":m['const'],"delta_url":m['Delta_URL'],"psp_code":"ap01"}
        # coll.insert_one(db_jj)
        
processing()   
# with DAG('PSP_water_calculations',start_date=dt(2025, 5, 1, 0, 0),
#     schedule_interval='0 4,10,16,22 * * *', catchup=False) as dag:
#     task = PythonSensor(
#         task_id="psp_water_calc",
#         python_callable=processing,
#         poke_interval=3600,  # 1 hr
#         timeout=10800,     # 3 hrs  
#         mode="poke",       
#         soft_fail=False
#     )


