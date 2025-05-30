from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import pytz
from weather_data_functions import checking_date_minIO, processing_files, send_mail

def failure_mail(context):
    execution_date = context.get('execution_date')
    execution_date = execution_date - timedelta(days=1)
    send_mail("Unable to retrieve the weather data after multiple attempts for the execution date: " + str(execution_date.strftime("%Y-%m-%d")) + " 12 UTC.")

with DAG('weather_data_processing_12UTC',start_date=datetime(2025, 5, 1, 0, 0),
    schedule_interval='40 8 * * *', catchup=False) as dag:

    ist = pytz.timezone('Asia/Kolkata')
    inputdate = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
    cycle = "12"

    branch_check = BranchPythonOperator(
    task_id="Cheking_files_in_MinIO",
    python_callable=checking_date_minIO,
    op_kwargs={"prefix":"weather_data/processed/"+str(inputdate)+"/"+"12_utc"}, 
    )

    task_if_true = PythonOperator(
        task_id="file_exists",
        python_callable=lambda: print("File exists"),
    )

    task_if_false = PythonSensor(
        task_id="file_missing",
        python_callable=processing_files,
        op_kwargs={"prefix": "weather_data/processed","inputdate":inputdate,"cycle":cycle},
        poke_interval=15,  # 30 mins
        timeout=30,     # 6 hours  
        mode="poke",       
        soft_fail=False,
        on_failure_callback=failure_mail
    )

    branch_check >> task_if_true
    branch_check >> task_if_false


