import pyarrow as pa

table = pa.table({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["NY", "LA", "Chicago"]
})

table_name = pa.read_table("parqut_path")  # reading parquet file

new_col = pa.array(["Engineer", "Doctor", "Artist"])
table = table.append_column("profession", new_col)   # Adding column

rename_map = {"name": "full_name", "city": "location"}
column_names = [rename_map.get(col, col) for col in table.schema.names]
table = table.rename_columns(column_names)                                 # Renaming the selected columns

table = table.rename_columns(["full_name", "age", "location"]) # Renaming the all columns

table = table.drop(["age"]) # dropping the column

table = table.select(["city", "name", "age"])  # Reorder and select required coluns 

table.slice(0, 5) # Selecting the rows

new_age = pa.array([26, 31, 36])
table = table.set_column(table.schema.get_field_index("age"), "age", new_age) # replacing the column

col = table["city"]            # Get column as ChunkedArray
values = table["age"].to_pylist()       # convert column to list

modified_values = [i-5 for i in values]
modified_values = pa.array(modified_values)
table = table.append_column("Modified age", modified_values)   # Modifying and adding age column

name_dict = {}
for idx, i in enumerate(table.schema.names):
    name_dict[table.schema.names[idx]] = table[i].to_pylist() # Converting all columns as keys and values as values

combined = pa.concat_tables([table, table2]) # Concate two tables

print(name_dict)

# Concat columns from different parquet files
df1 = pq.read_table("/home/gopi/doker_airflow/parquet_files/2m_relative_humidity_R1.parquet").to_pandas()
df2 = pq.read_table("/home/gopi/doker_airflow/parquet_files/2m_temperature_R1.parquet")
df3 = pq.read_table("/home/gopi/doker_airflow/parquet_files/surface_pressure_R1.parquet")
df4 = pq.read_table("/home/gopi/doker_airflow/parquet_files/total_rainfall_R1.parquet")

df2 = df2.select([3]).to_pandas()
df3 = df3.select([3]).to_pandas()
df4 = df4.select([3]).to_pandas()
df_final = pd.concat([df1, df2, df3, df4], axis=1)
print(len(df1), len(df2), len(df3), len(df4))
print(df_final)

import pyarrow.parquet as pq
import pyarrow.compute as pc

def day_wise_data(plant_lat, plant_lon):
    table = pq.read_table("/home/gopi/doker_airflow/parquet_files/2m_relative_humidity_R1.parquet")
    lat = table.column("lat")
    lon = table.column("lon")

    lat_lower = plant_lat - 0.5
    lat_upper = plant_lat + 0.5
    lon_lower = plant_lon - 0.25
    lon_upper = plant_lon + 0.25

    mask = (
        pc.and_(
            pc.and_(pc.greater(lat, lat_lower), pc.less(lat, lat_upper)),
            pc.and_(pc.greater(lon, lon_lower), pc.less(lon, lon_upper))
        )
    )

    filtered_table = table.filter(mask)
    
    filtered_df = filtered_table.to_pandas()
    grouped_df = filtered_df.groupby("time", as_index=False)["rh2m"].mean()
    print(grouped_df)

day_wise_data(8.8905, 77.7862)
