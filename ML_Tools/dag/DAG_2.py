from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import logging, boto3, io, os
from config import config_setup
import pyarrow.parquet as pa
import pandas as pd

logger = logging.getLogger("airflow.task")
bucket_name = config_setup['source_bucket']
s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

def print_received_value(**kwargs):
    number = kwargs['dag_run'].conf.get('date', 'No number received')
    number = str(20250505)
    dag_folder = os.path.dirname(__file__)
    files = os.listdir(dag_folder)
    logging.info(f"DAG task performing date:{number}")
    logging.info(f"DAG folder:{dag_folder}")
    logging.info(f"DAG folder:{files}")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/"+number+"/parquet")
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    df = pd.read_csv(dag_folder+"/Greenkowindplantdetails_OLD.csv")
    for i in lst:
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read()
        table = pa.read_table(io.BytesIO(body))
        logging.info(f"Processing parquet file length:{len(table)}")
    return number

with DAG('dag_2', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    doing_task_A = PythonOperator(
        task_id="file_exists",
        python_callable=print_received_value,
    )