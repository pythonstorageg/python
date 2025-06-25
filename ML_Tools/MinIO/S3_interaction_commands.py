import xarray as xr
import pyarrow.parquet as pa
import pandas as pd
import boto3

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

bucket_name = config_setup['source_bucket']

# Read files from S3
response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/20250519/parquet")
file_list = [obj["Key"] for obj in response.get("Contents", [])]

for i in file_list:
    if ".nc" in i:
        obj = s3.get_object(Bucket=bucket_name, Key="testing.nc")
        body = obj['Body'].read()
        ds_from_s3 = xr.open_dataset(io.BytesIO(body))
        df = ds_from_s3.to_dataframe().reset_index()

    if ".parquet" in i:
        print(i)
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read()
        table = pa.read_table(io.BytesIO(body))
        df = table.to_pandas()

    if ".csv" in i:
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(body))
        
        
        
# Upload files to S3

df to parquet save
------------------
table = pa.Table.from_pandas(df)
buffer = io.BytesIO()
pq.write_table(table, buffer)
buffer.seek(0)
s3.put_object(Bucket=bucket_name,Key=prefix+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue())

nc save after read
------------------
ds = xr.open_dataset("nc_file_path)
buffer = io.BytesIO()
data_bytes = ds.to_netcdf(buffer) 
s3.put_object(Bucket=bucket_name, Key=prefix+"/"+str(inputdate)+"/nc/"+file_name+".nc", Body=data_bytes)

df save
-------













