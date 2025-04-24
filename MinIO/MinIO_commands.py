
import boto3,os,io
import pandas as pd

bucket_name = "ml-flow-trails"

s3 = boto3.client(
    's3',
    endpoint_url='http://10.88.0.39:9000',
    aws_access_key_id='miniorootadmin',
    aws_secret_access_key='m1n10@r00t@Psw'
)

def upload_file(bucket_name,file_path,site_name):
    minIOPath_to_upload = "/solar/"+site_name+"/"+file_path.split("/")[-1]
    s3.upload_file(file_path, bucket_name, minIOPath_to_upload)

def any_file_exists(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    return any(lst)

def downlod_file(bucket_name,MiniIOPath_to_download,saving_path):
    s3.download_file(bucket_name, MiniIOPath_to_download, saving_path)

def list_the_files_with_prefix(bucket_name,prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    for obj in response.get('Contents', []):
        print("Found:", obj['Key'])

def processing_file_without_local_download(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    file_list = [obj["Key"] for obj in response.get("Contents", [])]
    if len(file_list)>0:
        for i in file_list:
            obj = s3.get_object(Bucket=bucket_name, Key=i)
            body = obj['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(body))
            df = df.drop(columns=['x','y'])
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer,index=False)
            s3.put_object(Bucket=bucket_name, Key=i.replace(".","_mdified."), Body=csv_buffer.getvalue())



