from sqlalchemy import create_engine,text
import sqlalchemy
import pandas as pd
import numpy as np
import pandas as pd
import pytz
import statsmodels.api as sm

ist = pytz.timezone('Asia/Kolkata')

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
    df_db['TIMESTAMP_IST'] = df_db['TIMESTAMP_IST'].dt.strftime('%d/%m/%y %H:%M')
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
    PSP_Data["P_N"] = PSP_Data["P_Total"].apply(lambda x: x / 245 if x > 0 else x / 256)
    PSP_Data["Net_Head_m"] = PSP_Data["UP_RES_L"] - PSP_Data["LO_RES_L"] - 2.5
    PSP_Data.sort_values(by="Net_Head_m", inplace=True)
    PSP_Data['Net_Head_m'] = pd.to_numeric(PSP_Data['Net_Head_m'], errors='coerce')
    gen_mode['G_Net_Head_m'] = pd.to_numeric(gen_mode['G_Net_Head_m'], errors='coerce')
    comb_psp = pd.merge_asof(PSP_Data, gen_mode, left_on="Net_Head_m", right_on="G_Net_Head_m")
    comb_psp = pd.merge_asof(comb_psp, pump_mode, left_on="Net_Head_m", right_on="P_Net_Head_m")
    comb_psp = comb_psp[comb_psp.ON_Units != 0]
    comb_psp.sort_values(by="DateTime_IntBegin", axis=0, ascending=True, inplace=True, ignore_index=True)
    comb_psp["Delta_V_mcm"] = comb_psp.apply(
        lambda row: row['G_Disch'] * row['P_N'] if row['P_N'] > 0 else -1 * row['P_Disch'] * row['P_N'],
        axis=1) * 15 * 60 / (10e5)
    comb_psp["Delta_URL"] = comb_psp["UP_RES_L"].diff()
    comb_psp["Machine_Hrs_Real"] = 0.25 * comb_psp["P_N"]
    comb_psp.index = comb_psp["DateTime_IntBegin"]
    comb_psp.drop(columns=["DateTime_IntBegin", "U1_Stat", "U2_Stat", "U3_Stat", "U4_Stat", "U5_Stat", "U6_Stat", "U7_Stat",
                        "U8_Stat", ], inplace=True)
    return comb_psp





engine = create_engine("postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database")

df_db = getting_data_from_DB(engine,'your_table_name','2025-06-15 18:30:00+00:00')
gen_mode,pump_mode = calculate_gen_pump_modes("/home/gopi/doker_airflow/hydro/Pinnapuram_PT126_PT208_LARGE_SMALL_Export_Pmax_PU_TU.xlsx",
                                              "PINNA_LARGE_TU","PINNA_LARGE_PU")

df_old = pd.read_excel("/home/gopi/doker_airflow/hydro/comb_psp.xlsx",
                         sheet_name="comb_psp", header=0)
df_old = df_old[['DateTime_IntBegin','UNIT_1_P','UNIT_2_P','UNIT_3_P','UNIT_4_P','UNIT_5_P','UNIT_6_P','UNIT_7_P','UNIT_8_P',
         'UP_RES_L','LO_RES_L']]

PSP_Data = pd.concat([df_old, df_db], ignore_index=True)
PSP_Data['DateTime_IntBegin'] = pd.to_datetime(PSP_Data['DateTime_IntBegin'], errors='coerce')
PSP_Data.sort_values(by="DateTime_IntBegin", ascending=True, inplace=True, ignore_index=True)
PSP_Data.index = PSP_Data["DateTime_IntBegin"]
PSP_Data = PSP_Data.drop_duplicates(keep="last")

comb_psp_df = comb_psp_data(PSP_Data,gen_mode,pump_mode)

comb_psp_df.to_csv("/home/gopi/doker_airflow/hydro/comb_psp.csv")

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