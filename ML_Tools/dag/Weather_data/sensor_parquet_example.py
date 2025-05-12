from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy import DummyOperator
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from processing_poovani_weatherData import plant_wise_data_process

bucket_name = config_setup['source_bucket']

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

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

def checking_date_minIO(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    for i in lst:
        if str(inputdate) in i:
            return "file_exists"
    return "file_missing"

# def Plant_wise_processing():
#     plant_wise_data_process(inputdate)
    
def processing_files(bucket_name,prefix):
    url = config_setup["weather_data"]["url"]
    api_key = config_setup["weather_data"]["api_key"]
    username = config_setup["weather_data"]["username"]
    password = config_setup["weather_data"]["password"]
    files = [
            {'url': url, 'variable': "GreenkoWindEnergy"},
            {'url': url, 'variable': "wind_solar_ind"},  
        ]
    with requests.Session() as session:
        session.auth = HTTPBasicAuth(username, password)     
        for file in files:
            try:
                if 'wind_solar_ind' in file['variable']:
                    # subdir_name = "wind_solar_ind"
                    continue
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
            # if 'Content-Disposition' in response.headers:
            #     cd = response.headers['Content-Disposition']
            #     if 'filename=' in cd:
            #         filename = cd.split('filename=')[1].strip('"')
            #     else:
            #         filename = file['variable'] 
            # else:
            #     filename = file['variable'] 
            if response.status_code == 200:
                prev_file_name = None
                uploaded_table = None
                MLFLOW_TRACKING_URI = config_setup['MLFLOW']['tracking_uri']
                EXPERIMENT_NAME = config_setup['MLFLOW']['experiment_name']
                mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
                os.environ["AWS_ACCESS_KEY_ID"] = config_setup['access_key']
                os.environ["AWS_SECRET_ACCESS_KEY"] = config_setup['secret_key']
                os.environ["MLFLOW_S3_ENDPOINT_URL"] = config_setup['minio_endpoint']
                mlflow.set_experiment(EXPERIMENT_NAME)
                with mlflow.start_run(run_name=str(inputdate)):
                    archive_bytes = io.BytesIO(response.content)
                    archive_bytes.seek(0)  # Reset stream
                    html_files = []
                    with tarfile.open(fileobj=archive_bytes, mode='r:gz') as tar:
                        for member in tar.getmembers():
                            try:
                                if member.isfile():
                                    file_name = ("_").join(member.name.split("_")[:-1])
                                    extracted_file = tar.extractfile(member)
                                    file_data = extracted_file.read()
                                    file_data = io.BytesIO(file_data)
                                    ds = xr.open_dataset(file_data)
                                    df = ds.to_dataframe().reset_index() 
                                    s3.upload_fileobj(file_data, bucket_name, prefix+"/"+str(inputdate)+"/nc/"+member.name) 

                                    table = pyarrow.Table.from_pandas(df)
                                    if (prev_file_name == None) or (prev_file_name!=file_name):
                                        uploaded_table = table
                                        buffer = io.BytesIO()
                                        pa.write_table(uploaded_table, buffer)  
                                        buffer.seek(0)
                                        s3.put_object(Bucket=bucket_name,Key= "weather_data/processed"+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue())
                                        print("file uploaded with none",file_name+".parquet",len(uploaded_table))
                                        prev_file_name = file_name
                                    else:
                                        if prev_file_name == file_name:
                                            uploaded_table = pyarrow.concat_tables([uploaded_table, table]) 
                                            buffer = io.BytesIO()
                                            pa.write_table(uploaded_table, buffer)  
                                            buffer.seek(0)
                                            s3.put_object(Bucket=bucket_name,Key= "weather_data/processed"+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue()) 
                                            print("file uploaded after concatenation",file_name+".parquet",len(uploaded_table))

                                    # html_path = f"/tmp/"+member.name.replace(".nc",".html")
                                    # profile = ProfileReport(df, title="EDA Report", explorative=True)
                                    # profile.to_file(html_path)
                                    # html_files.append(html_path)
                                    # mlflow.log_artifact(html_path, "Raw data Visualization")        
                            except:
                                continue        
                    for html_file in html_files:
                        if os.path.exists(html_file):
                            os.remove(html_file)
            else:
                print("downloaded")

with DAG('sensor_parquet_testing',start_date=datetime(2025, 5, 1, 0, 0),
    schedule_interval='40 8 * * *', catchup=True) as dag:

    branch_check = BranchPythonOperator(
    task_id="Cheking_files_in_MinIO",
    python_callable=checking_date_minIO,
    op_kwargs={"bucket_name": bucket_name, "prefix": "weather_data/processed"},
    )

    task_if_true = PythonOperator(
        task_id="file_exists",
        python_callable=lambda: print("File exists"),
    )

    task_if_false = PythonOperator(
        task_id="file_missing",
        python_callable=processing_files,
        op_kwargs={"bucket_name": bucket_name, "prefix": "weather_data/processed"},
    )

    task_plant_wise = PythonOperator(
        task_id="plant_wise_pocessing",
        python_callable=plant_wise_data_process,
        op_kwargs={"date": inputdate}
    )

    branch_check >> task_if_true
    branch_check >> task_if_false
    task_if_false >> task_plant_wise



