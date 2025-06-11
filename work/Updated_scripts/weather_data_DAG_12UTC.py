# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.sensors.python import PythonSensor
# from datetime import datetime, timedelta
# import pytz
# # from weather_data_functions import checking_date_minIO, processing_files, send_mail
# from weather_data_functions_phgtv import checking_date_minIO, processing_files, send_mail


# def failure_mail(context):
#     execution_date = context['params'].get('inputdate', 'N/A')
#     execution_date = datetime.strptime(execution_date, "%Y%m%d")    
#     formatted_date = execution_date.strftime("%Y-%m-%d")
#     send_mail("Unable to retrieve the weather data after multiple attempts for the execution date: " + formatted_date + " 12 UTC.")

# with DAG('weather_data_processing_12UTC',start_date=datetime(2025, 5, 1, 0, 0),
#     schedule_interval='30 20 * * *', catchup=False) as dag:

#     ist = pytz.timezone('Asia/Kolkata')
#     inputdate = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
#     cycle = "12"

#     branch_check = BranchPythonOperator(
#     task_id="Cheking_files_in_MinIO",
#     python_callable=checking_date_minIO,
#     op_kwargs={"prefix":"weather_data/processed/"+str(inputdate)+"/"+"12_utc"}, 
#     )

#     task_if_true = PythonOperator(
#         task_id="file_exists",
#         python_callable=lambda: print("File exists"),
#     )

#     task_if_false = PythonSensor(
#         task_id="file_missing",
#         python_callable=processing_files,
#         op_kwargs={"prefix": "weather_data/processed","inputdate":inputdate,"cycle":cycle},
#         params={"inputdate": inputdate},
#         poke_interval=3600,  # 1 hr
#         timeout=32400,     # 9 hrs  
#         mode="poke",       
#         soft_fail=False,
#         on_failure_callback=failure_mail
#     )

#     branch_check >> task_if_true
#     branch_check >> task_if_false


