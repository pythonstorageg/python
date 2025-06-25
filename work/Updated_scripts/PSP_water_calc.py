# from airflow import DAG
# from airflow.sensors.python import PythonSensor
from sqlalchemy import create_engine
import os,pytz,yaml
from PSP_water_calc_functions import getting_data_timestamp_sink_table,getting_data_source_table,calculate_gen_pump_modes,comb_psp_data,append_parameters,export_data_sink_table,plotting_xy_scatter
from datetime import timedelta,datetime
import pandas as pd
import statsmodels.api as sm

ist = pytz.timezone('Asia/Kolkata')

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.yaml"), "r") as file:
    config = yaml.safe_load(file)

engine = create_engine(config['time_scale']['url'])

def processing():
    # try:
        curent_time = datetime.now(ist)
        df_old,latest_time = getting_data_timestamp_sink_table(engine,config['time_scale']['sink_table'])
        # df_old = df_old[['DateTime_IntBegin','UNIT_1_P','UNIT_2_P','UNIT_3_P','UNIT_4_P','UNIT_5_P','UNIT_6_P','UNIT_7_P','UNIT_8_P',
        #             'UP_RES_L','LO_RES_L']]
        df_old.to_csv("/home/gopi/doker_airflow/Updated_dag_scripts/comb_psp_jun_02_25.csv")
processing()
    #     try:
    #         start_time = (latest_time+timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    #         df_new = getting_data_source_table(engine,config['time_scale']['source_table'],start_time)
    #         PSP_Data = pd.concat([df_old,df_new], ignore_index=True)
    #     except:
    #         print("failed to retrieve the latest data")
    #         PSP_Data = df_old    

    #     PSP_Data['DateTime_IntBegin'] = pd.to_datetime(PSP_Data['DateTime_IntBegin'], errors='coerce')
    #     PSP_Data = PSP_Data.drop_duplicates(subset='DateTime_IntBegin', keep='first')

    #     gen_mode,pump_mode = calculate_gen_pump_modes(os.path.join(os.path.dirname(os.path.abspath(__file__)),"Pinnapuram_PT126_PT208_LARGE_SMALL_Export_Pmax_PU_TU.xlsx"),
    #                                                         "PINNA_LARGE_TU","PINNA_LARGE_PU")
        
    #     comb_psp_df = comb_psp_data(PSP_Data,gen_mode,pump_mode)
    #     comb_psp_df["Delta_URL"]=comb_psp_df["Delta_URL"].bfill()
    #     print(comb_psp_df)
    #     Q1 = comb_psp_df["Delta_URL"].quantile(0.05)
    #     Q3 = comb_psp_df["Delta_URL"].quantile(0.95)
    #     outliers = comb_psp_df[(comb_psp_df["Delta_URL"] < Q1) | (comb_psp_df["Delta_URL"] > Q3)]
    #     comb_psp_df = comb_psp_df.drop(outliers.index)
    #     print(comb_psp_df.describe().to_string())
    #     X=comb_psp_df["Delta_URL"]
    #     X = sm.add_constant(X)
    #     reg = sm.OLS(comb_psp_df["Machine_Hrs_Real"],X,hasconst=True)
    #     reg_f = reg.fit()
    #     print(reg_f.params)
    #     print("R²:", reg_f.rsquared)
    #     m= reg_f.params
    #     print(m)
    #     parameters_df= pd.DataFrame([{'timestamp': curent_time,'m': m['Delta_URL'],'c': m['const'],'r2': reg_f.rsquared}])
    #     print("appending paameters")
    #     append_parameters(parameters_df,config['time_scale']['parameters_table'],engine)
    #     print("replacing df ")
    #     export_data_sink_table(comb_psp_df,config['time_scale']['sink_table'],engine)
    #     print("process completed")
    #     return True
    # except:
    #     return False

# with DAG('PSP_water_calculations',start_date=datetime(2025, 5, 1, 0, 0),
#     schedule_interval="25 18 * * *", catchup=False) as dag:
#     task = PythonSensor(
#         task_id="psp_water_calc",
#         python_callable=processing,
#         poke_interval=3600,  # 1 hr
#         timeout=10800,     # 3 hrs  
#         mode="poke",       
#         soft_fail=False
#     )
