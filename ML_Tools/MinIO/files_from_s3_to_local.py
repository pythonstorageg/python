


response = s3.list_objects_v2(Bucket=bucket_name, Prefix="weather_data/processed/"+number+"/nc")
lst = [obj["Key"] for obj in response.get("Contents", [])]

# Parquet
for i in lst:
	obj = s3.get_object(Bucket=bucket_name, Key=i)
	body = obj['Body'].read()
	table = pa.read_table(io.BytesIO(body))
	pa.write_table(table, "/home/gopi/doker_airflow/nc/"+i.split("/")[-1])

# nc
for i in lst:
	obj = s3.get_object(Bucket=bucket_name, Key=i)
	body = obj['Body'].read()
	with open("/home/gopi/doker_airflow/nc/"+i.split("/")[-1], "wb") as f:
	f.write(body)
