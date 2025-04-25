from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, boto3, io
import pandas as pd
import pyarrow.parquet as pa

bucket_name = "ml-flow-trails"

s3 = boto3.client(
    's3',
    endpoint_url='http://10.88.0.39:9000',
    aws_access_key_id='miniorootadmin',
    aws_secret_access_key='m1n10@r00t@Psw'
)

def any_file_exists(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    return any(lst)

def processing_files(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    file_list = [obj["Key"] for obj in response.get("Contents", [])]
    if len(file_list)>0:
        for i in file_list:
            obj = s3.get_object(Bucket=bucket_name, Key=i)
            body = obj['Body'].read()
            table = pa.read_table(io.BytesIO(body))
            table = table.drop(['x','y'])
            parquet_buffer = io.BytesIO()
            pa.write_table(table, parquet_buffer, compression='snappy')
            s3.put_object(Bucket=bucket_name, Key=i.replace(".","_mdified."), Body=parquet_buffer.getvalue())

with DAG('sensor_parquet_testing', start_date=datetime(2025, 4, 23), schedule_interval='@daily') as dag:

    wait_for_any_file = PythonSensor(
        task_id="wait_for_any_file",
        python_callable=any_file_exists,
        poke_interval=30,    
        timeout=600,   
        mode='poke',
        op_kwargs={"bucket_name": bucket_name, "prefix": "solar"}
    )

    process_file = PythonOperator(
    task_id='process_file',
    python_callable=processing_files,
    op_kwargs={"bucket_name": "ml-flow-trails", "prefix": "solar"}
    )

    wait_for_any_file >> process_file



