from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from config import config_setup
import pytz
from processing_poovani_weatherData import plant_wise_data_process,checking_date_minIO,processing_files

bucket_name = config_setup['source_bucket']

ist = pytz.timezone('Asia/Kolkata')
yesterday = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
today = datetime.now(ist).strftime('%Y%m%d')
current_time = datetime.now(ist).strftime("%H")
if int(current_time) < 11:
    inputdate= yesterday  
    cycle = "12" 
else:
    inputdate = today 
    cycle = "00"

with DAG('sensor_parquet_testing',start_date=datetime(2025, 5, 1, 0, 0),
    schedule_interval='40 8 * * *', catchup=True) as dag:

    branch_check = BranchPythonOperator(
    task_id="Cheking_files_in_MinIO",
    python_callable=checking_date_minIO,
    op_kwargs={"bucket_name": bucket_name, "prefix": "weather_data/processed","inputdate":inputdate},  # bucket_name, prefix,inputdate
    )

    task_if_true = PythonOperator(
        task_id="file_exists",
        python_callable=lambda: print("File exists"),
    )

    task_if_false = PythonOperator(
        task_id="file_missing",
        python_callable=processing_files,
        op_kwargs={"bucket_name": bucket_name, "prefix": "weather_data/processed","inputdate":inputdate,"cycle":cycle}, # bucket_name,prefix,inputdate,cycle
    )

    task_plant_wise = PythonOperator(
        task_id="plant_wise_pocessing",
        python_callable=plant_wise_data_process,  
        op_kwargs={"bucket_name":bucket_name,"prefix":"weather_data/processed/"+str(inputdate)} # bucket_name,prefix
    )

    branch_check >> task_if_true
    branch_check >> task_if_false
    task_if_false >> task_plant_wise



