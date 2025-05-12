import pyarrow.compute as pc
import numpy as np
import boto3,io,os,pyarrow,mlflow
from config import config_setup
import pandas as pd
import pyarrow.parquet as pa
import pandas as pd
import pyarrow.parquet as pa
from config import config_setup
from ydata_profiling import ProfileReport
import pandas as pd
import xarray as xr
import requests, tarfile
from requests.auth import HTTPBasicAuth

s3 = boto3.client(
    's3',
    endpoint_url=config_setup['minio_endpoint'],
    aws_access_key_id=config_setup['access_key'],
    aws_secret_access_key=config_setup['secret_key']
)

def checking_date_minIO(bucket_name, prefix,inputdate):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    lst = [obj["Key"] for obj in response.get("Contents", [])]
    for i in lst:
        if str(inputdate) in i:
            return "file_exists"
    return "file_missing"

def processing_files(bucket_name,prefix,inputdate,cycle):
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
                                        s3.put_object(Bucket=bucket_name,Key= prefix+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue())
                                        print("file uploaded with none",file_name+".parquet",len(uploaded_table))
                                        prev_file_name = file_name
                                    else:
                                        if prev_file_name == file_name:
                                            uploaded_table = pyarrow.concat_tables([uploaded_table, table]) 
                                            buffer = io.BytesIO()
                                            pa.write_table(uploaded_table, buffer)  
                                            buffer.seek(0)
                                            s3.put_object(Bucket=bucket_name,Key= prefix+"/"+str(inputdate)+"/parquet/"+file_name+".parquet",Body=buffer.getvalue()) 
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

def interpolate_values(table, height, lat, lon):
    groups = table.column("time").to_pylist()
    x_col = table.column("lev").to_numpy()
    try:
        h_col = table.column("u").to_numpy()
        vel = 'u'
    except:
        h_col = table.column("v").to_numpy()
        vel = 'v'
    unique_groups = np.unique(groups)
    group_indices = {group: np.where(np.array(groups) == group)[0] for group in unique_groups}
    result = []
    for group in unique_groups:
        indices = group_indices[group]
        x_group = x_col[indices]
        h_group = h_col[indices]
        h_interp = float(np.interp(height, x_group, h_group))
        result.append({
            "group": group,
            "lat": lat,
            "lon": lon,
            "x": height,
            "h": h_interp
        })
    columns = {
        "time": [row["group"] for row in result],
        "lat": [row["lat"] for row in result],
        "lon": [row["lon"] for row in result],
        vel: [row["h"] for row in result]
    }
    result_table = pyarrow.table(columns)
    return result_table

def plant_wise_data_process(bucket_name,prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix+"/parquet")
    lst = [obj["Key"] for obj in response.get("Contents", [])]   
    dictnry = {} 
    parameter_rename = {"time":"time","lat":"latitude","lon":"longitude","rf":"Rain_Fall","dswrf":"Solar_Radiation","rh2m":"Relative_Humidity","press":"Pressure","t2m":"Temperature",
                          "u":"u_velocity","v":"v_velocity"}
    print("Getting parquet files data from MinIO")
    for i in lst:
        print("getting data:",i)
        obj = s3.get_object(Bucket=bucket_name, Key=i)
        body = obj['Body'].read()
        table = pa.read_table(io.BytesIO(body))
        dictnry[i.split("/")[-1]] = table
        print("Data adding to dictnry")
    print("Reading sites lat lon values")
    dag_folder = os.path.dirname(__file__)
    df = pd.read_csv(dag_folder+"/Greenkowindplantdetails_OLD.csv")
    for idx, plant_name in enumerate(df["Farm"]):
        plant_lat = df["latitude"][idx]
        plant_lon = df["longitude"][idx]
        plant_height = df["Hhgt"][idx]
        print("Processing",plant_name,plant_lat,plant_lon)
        combined_table = None
        nearest_lat = None
        nearest_lon = None
        for parameter in dictnry.keys():
            table = dictnry[parameter]
            lat_diff = pc.abs(pc.subtract(table["lat"], plant_lat))
            lon_diff = pc.abs(pc.subtract(table["lon"], plant_lon))
            total_diff = pc.add(lat_diff, lon_diff)
            min_index = np.argmin(total_diff.to_numpy())
            nearest_row = table.slice(min_index, 1)
            nearest_lat = nearest_row.column("lat")[0].as_py()
            nearest_lon = nearest_row.column("lon")[0].as_py()
            lat_mask = pc.equal(table["lat"], nearest_lat)
            lon_mask = pc.equal(table["lon"], nearest_lon)
            combined_mask = pc.and_(lat_mask, lon_mask)
            filtered_table = table.filter(combined_mask)
            if "_wind" in parameter:
                filtered_table = interpolate_values(filtered_table,plant_height,nearest_lat,nearest_lon)
            if combined_table == None:
                combined_table = filtered_table
            else:
                new_column = filtered_table.column(3)
                new_column_name = filtered_table.schema.names[3]
                combined_table = combined_table.append_column(new_column_name, new_column)
        if combined_table:
            old_names = combined_table.schema.names        
            new_names = [parameter_rename.get(name, name) for name in old_names] 
            combined_table = combined_table.rename_columns(new_names)    
            new_temp_col = pc.subtract(combined_table["Temperature"], pyarrow.scalar(273))
            combined_table = combined_table.set_column(combined_table.schema.get_field_index("Temperature"), "Temperature", new_temp_col)
            buffer = io.BytesIO()
            pa.write_table(combined_table, buffer)
            buffer.seek(0)
            s3.put_object(Bucket=bucket_name,Key=prefix+"/Plant_wise_data/"+plant_name+"_"+str(plant_lat)+"_"+str(plant_lon)+".parquet",Body=buffer.getvalue())        