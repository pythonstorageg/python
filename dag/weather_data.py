from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pyarrow, os, boto3, io, mlflow
import pandas as pd
import pyarrow.parquet as pa
from config import config_setup
from ydata_profiling import ProfileReport
import pandas as pd
import xarray as xr
import pytz, requests, tarfile
from requests.auth import HTTPBasicAuth

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

url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"
api_key = 'S447KGpOXCJkBTwCiDB6N0bryryGRBij'
files = [
        {'url': url, 'variable': "GreenkoWindEnergy"},
        {'url': url, 'variable': "wind_solar_ind"},  
    ]

username = 'greenko'
password = 'GreenKo@ncmr9#'
ist = pytz.timezone('Asia/Kolkata')

def checking_date_minIO(bucket_name, prefix):
    yesterday = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
    today = datetime.now(ist).strftime('%Y%m%d')
    current_time = datetime.now(ist).strftime("%H")
    if current_time < 11:
        inputdate= yesterday  
    else:
        inputdate = today 
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    for i in lst:
        if "processed/"+str(inputdate) in i:
            return True
    return False
    
def process_parquet_files(parquet_file_name,df):
    file_name = os.path.basename(parquet_file_name)
    with mlflow.start_run(run_name=f"process_{file_name}"):
        try:
            # df = file_data.to_pandas()
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

def processing_files(bucket_name,prefix):
    yesterday = (datetime.now(ist) - timedelta(1)).strftime('%Y%m%d')
    today = datetime.now(ist).strftime('%Y%m%d')
    current_time = datetime.now(ist).strftime("%H")
    if current_time < 11:
        inputdate= yesterday 
        cycle = "12" 
    else:
        inputdate = today
        cycle = "00"
    with requests.Session() as session:
        session.auth = HTTPBasicAuth(username, password)     
        for file in files:
            try:
                if 'wind_solar_ind' in file['variable']:
                    subdir_name = "wind_solar_ind"
                if 'GreenkoWindEnergy' in file['variable']:
                    subdir_name = "GreenkoWindEnergy"
            except Exception as e:
                print(e)
            headers = {
                'inputdate': inputdate,
                'cycle': cycle,
                'datavariable': subdir_name,
                'api-key': api_key,
            }
            try: 
                response = session.post(file['url'], headers=headers, stream=True)
            except Exception as e:
                continue
            if 'Content-Disposition' in response.headers:
                cd = response.headers['Content-Disposition']
                if 'filename=' in cd:
                    filename = cd.split('filename=')[1].strip('"')
                else:
                    filename = file['variable'] 
            else:
                filename = file['variable'] 
            if response.status_code == 200:
                archive_bytes = io.BytesIO(response.content)
                archive_bytes.seek(0)  # Reset stream
                with tarfile.open(fileobj=archive_bytes, mode='r:gz') as tar:
                    for member in tar.getmembers():
                        if member.isfile():
                            extracted_file = tar.extractfile(member)
                            file_data = extracted_file.read()
                            file_data = io.BytesIO(file_data)
                            ds = xr.open_dataset(file_data)
                            df = ds.to_dataframe().reset_index()   
                            process_parquet_files(filename,df)   
                            s3.upload_fileobj(file_data, bucket_name, prefix+"/"+str(inputdate)+"/"+member.name) 
            else:
                print("downloaded")

with DAG('sensor_parquet_testing',start_date=datetime(2025, 5, 1, 0, 0),
    schedule_interval='40 8 * * *', catchup=True) as dag:

    branch_check = BranchPythonOperator(
    task_id="branch_check_file",
    python_callable=checking_date_minIO,
    op_kwargs={"bucket_name": bucket_name, "prefix": "processed"},
    )

    task_if_true = PythonOperator(
        task_id=True,
        python_callable=lambda: print("File exists"),
    )

    task_if_false = PythonOperator(
        task_id=False,
        python_callable=checking_date_minIO,
        op_kwargs={"bucket_name": bucket_name, "prefix": "processed"},
    )

    branch_check >> [task_if_true, task_if_false]



