from sqlalchemy import text,inspect,types
import pandas as pd
import numpy as np
import pandas as pd
import pytz
from datetime import datetime as dt
import matplotlib.pyplot as plt

ist = pytz.timezone('Asia/Kolkata')

def check_table_timscaleDB(engine,table_name):
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        return True
    else:
        return False

def getting_data_timestamp_sink_table(engine,table_name):
    query = text(f"""
    SELECT *
    FROM "{table_name}" """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['DateTime_IntBegin'] = pd.to_datetime(df['DateTime_IntBegin'], errors='coerce')
    latest_time = df['DateTime_IntBegin'].max()
    df['DateTime_IntBegin'] = df['DateTime_IntBegin'].dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
    return df,latest_time

def getting_data_source_table(engine,table_name,start_time):
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
    PSP_Data = PSP_Data[(abs(PSP_Data['P_Total']) >= 100) & (PSP_Data['UP_RES_L'].notna())].copy()
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
    same_sign_mask = (((df['Delta_URL'] > 0) & (df['P_Total'] > 0)) | ((df['Delta_URL'] < 0) & (df['P_Total'] < 0)) | (df['Delta_URL'] == 0))
    comb_psp = comb_psp[~same_sign_mask]  
    comb_psp.index = comb_psp["DateTime_IntBegin"]
    comb_psp.drop(columns=["DateTime_IntBegin", "U1_Stat", "U2_Stat", "U3_Stat", "U4_Stat", "U5_Stat", "U6_Stat", "U7_Stat",
                        "U8_Stat",'time_diff'], inplace=True)
    return comb_psp

def export_data_sink_table(comb_psp_df,sink_table,engine):
    comb_psp_df.reset_index(inplace=True)
    comb_psp_df['DateTime_IntBegin'] = pd.to_datetime(
    comb_psp_df['DateTime_IntBegin'], 
    format='%d/%m/%Y %H:%M:%S',  # adjust format if needed
    errors='coerce'
    ).dt.tz_localize('Asia/Kolkata')

    comb_psp_df.to_sql(
    name=sink_table,
    con=engine,
    if_exists='replace',
    index=False,
    dtype={
        'DateTime_IntBegin': types.TIMESTAMP(timezone=True)  # for TIMESTAMPTZ
    }
    )

def append_parameters(parameters_df,parameter_table,engine): 
    parameters_df.to_sql(
    name=parameter_table,
    con=engine,
    if_exists='append',
    index=False,
    dtype={
        'DateTime_IntBegin': types.TIMESTAMP(timezone=True)  # for TIMESTAMPTZ
    }
    )

def upload_df_to_timescale(engine,df,table_name):
    df['DateTime_IntBegin'] = pd.to_datetime(
        df['DateTime_IntBegin'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    ).dt.tz_localize('Asia/Kolkata')
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace', 
        index=False,
        dtype={
            'DateTime_IntBegin': types.TIMESTAMP(timezone=True)
        }
    )

def plotting_xy_scatter(df,x,y,m,c,r2):
    x_vals = df[x].values
    y_vals = df[y].values

    line_x = np.linspace(-0.5, 0.5, 200)
    line_y = m * line_x + c

    plt.figure(figsize=(8, 6))
    plt.scatter(x_vals, y_vals, label='Data points', alpha=0.6)
    plt.plot(line_x, line_y, color='red', label=f'Line: y = {m:.2f}x + {c:.2f}')
    
    equation = f'y = {m:.2f}x + {c:.2f}'
    xmin, xmax = plt.xlim()
    ymin, ymax = plt.ylim()
    plt.text(xmin + 0.05*(xmax - xmin), ymin + 0.95*(ymax - ymin), equation,
             fontsize=12, color='red', bbox=dict(facecolor='white', alpha=0.6))
    
    plt.text(0.05, ymin + 0.8 * (ymax - ymin),
             f'R² = {r2:.3f}',
             fontsize=12, color='blue',
             bbox=dict(facecolor='white', alpha=0.6))
    
    plt.scatter(df[x], df[y])
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f'Scatter Plot of {x} vs {y} (Plant_data)')
    plt.grid(True)
    plt.savefig('scatter_plot.jpg', format='jpg', dpi=300)

# from sqlalchemy import create_engine,text,types
# import os,yaml
# with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.yaml"), "r") as file:
#     config = yaml.safe_load(file)
# engine = create_engine(config['time_scale']['url'])
# df = pd.read_csv("/home/gopi/doker_airflow/hydro/comb_psp_plant.csv")
# upload_df_to_timescale(engine,df,'PSP_water_calculation')
