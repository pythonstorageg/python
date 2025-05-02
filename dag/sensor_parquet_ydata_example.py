from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pyarrow, os, boto3, io, mlflow
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pyarrow.parquet as pa
import plotly.express as px
import plotly.graph_objects as go
from config import config_setup
from ydata_profiling import ProfileReport
import pandas as pd

bucket_name = config_setup['source_bucket']

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

MLFLOW_TRACKING_URI = config_setup['MLFLOW']['tracking_uri']
EXPERIMENT_NAME = config_setup['MLFLOW']['experiment_name']
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

def any_file_exists(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    return any(lst)

def process_parquet_files(parquet_file_name,file_data):
    file_name = os.path.basename(parquet_file_name)
    with mlflow.start_run(run_name=f"process_{file_name}"):
        try:
            df = file_data.to_pandas()
            mlflow.log_param("num_rows", len(df))
            mlflow.log_param("num_columns", len(df.columns))
            html_files = []
            html_path = f"/tmp/generated_file.html"
            profile = ProfileReport(df, title="EDA Report", explorative=True)
            profile.to_file(html_path)
            html_files.append(html_path)
            mlflow.log_artifact(html_path, "visualizations")
            for html_file in html_files:
                if os.path.exists(html_file):
                    os.remove(html_file)
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")
            mlflow.log_param("error", str(e))

def processing_files(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    file_list = [obj["Key"] for obj in response.get("Contents", [])]
    if len(file_list)>0:
        for i in file_list:
            if (".csv" in i) or (".CSV" in i):
                obj = s3.get_object(Bucket=bucket_name, Key=i)
                body = obj['Body'].read().decode('utf-8')
                table = pyarrow.Table.from_pandas(pd.read_csv(io.StringIO(body)))
                process_parquet_files("solar/"+i,table)
                s3.put_object(Bucket=bucket_name, Key="/processed/csv_files/"+(i.split("/")[-1]), Body=body)
                s3.delete_object(Bucket=bucket_name, Key=i)
            else:    
                obj = s3.get_object(Bucket=bucket_name, Key=i)
                body = obj['Body'].read()
                table = pa.read_table(io.BytesIO(body))
                process_parquet_files("solar/"+i,table)
                s3.put_object(Bucket=bucket_name, Key="/processed/parquet_files/"+(i.split("/")[-1]), Body=body)
                s3.delete_object(Bucket=bucket_name, Key=i)

with DAG('sensor_parquet_testing',start_date=datetime(2025, 4, 29),
    schedule_interval='0 7 15 * *'  ) as dag:

    wait_for_any_file = PythonSensor(
        task_id="wait_for_any_file",
        python_callable=any_file_exists,
        poke_interval=30,    
        timeout=600,   
        mode='reschedule',
        op_kwargs={"bucket_name": bucket_name, "prefix": "uploads"}
    )

    process_file = PythonOperator(
    task_id='process_file',
    python_callable=processing_files,
    op_kwargs={"bucket_name": bucket_name, "prefix": "uploads"}
    )

    wait_for_any_file >> process_file
