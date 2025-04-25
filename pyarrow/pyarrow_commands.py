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
