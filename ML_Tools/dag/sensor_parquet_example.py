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

bucket_name = config_setup['source_bucket']

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

MLFLOW_TRACKING_URI = config_setup['MLFLOW']['tracking_uri']
EXPERIMENT_NAME = config_setup['MLFLOW']['experiment_name']

def any_file_exists(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    return any(lst)

def process_parquet_files(parquet_file_name,file_data):
    current_timestamp = datetime.now()
    print("yes")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        experiment_id = mlflow.create_experiment(EXPERIMENT_NAME)
    except:
        experiment_id = mlflow.get_experiment_by_name(EXPERIMENT_NAME).experiment_id
    mlflow.set_experiment(EXPERIMENT_NAME)
    file_name = os.path.basename(parquet_file_name)
    with mlflow.start_run(run_name=f"process_{file_name}"):
        # Log the source file
        mlflow.log_param("source_file", file_name)
        try:
            df = file_data.to_pandas()
            mlflow.log_param("num_rows", len(df))
            mlflow.log_param("num_columns", len(df.columns))
            # Generate visualizations based on data types
            html_files = []
            # Create a histogram for each numeric column
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
                fig = px.histogram(df, x=col, title=f"Histogram of {col}")
                html_path = f"/tmp/histogram_{col}.html"
                fig.write_html(html_path)
                mlflow.log_artifact(html_path, "visualizations")  # Log artifact
                html_files.append(html_path)

            # Create a scatter plot if there are at least 2 numeric columns
            if len(numeric_cols) >= 2:
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], 
                                title=f"Scatter Plot: {numeric_cols[0]} vs {numeric_cols[1]}")
                html_path = f"/tmp/scatter_plot.html"
                fig.write_html(html_path)
                mlflow.log_artifact(html_path, "visualizations")
                html_files.append(html_path)

            # Create a correlation heatmap if there are multiple numeric columns
            if len(numeric_cols) > 1:
                corr = df[numeric_cols].corr()
                fig = go.Figure(data=go.Heatmap(
                    z=corr.values,
                    x=corr.columns,
                    y=corr.columns,
                    colorscale='Viridis'))
                fig.update_layout(title="Correlation Heatmap")
                html_path = f"/tmp/correlation_heatmap.html"
                fig.write_html(html_path)
                mlflow.log_artifact(html_path, "visualizations")
                html_files.append(html_path)

            # Create a bar chart for categorical columns (if any)
            categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
            for col in categorical_cols[:3]:  # Limit to first 3 categorical columns
                value_counts = df[col].value_counts().head(10)  # Top 10 categories
                fig = px.bar(x=value_counts.index, y=value_counts.values, 
                            title=f"Top Categories in {col}")
                html_path = f"/tmp/bar_chart_{col}.html"
                fig.write_html(html_path)
                mlflow.log_artifact(html_path, "visualizations")
                html_files.append(html_path)

            # Create a time series plot if there's a datetime column
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            if date_cols and numeric_cols:
                try:
                    df[date_cols[0]] = pd.to_datetime(df[date_cols[0]])
                    fig = px.line(df, x=date_cols[0], y=numeric_cols[0], 
                                title=f"Time Series: {numeric_cols[0]} over {date_cols[0]}")
                    html_path = f"/tmp/time_series.html"
                    fig.write_html(html_path)
                    mlflow.log_artifact(html_path, "visualizations")
                    html_files.append(html_path)
                except Exception as e:
                    print(f"Could not convert {date_cols[0]} to datetime format: {e}")

            # Remove HTML files after logging
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


