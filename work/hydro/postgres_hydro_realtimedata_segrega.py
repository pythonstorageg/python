from sqlalchemy import create_engine,text
import sqlalchemy
import pandas as pd
import numpy as np


engine = create_engine("postgresql+psycopg2://gopi:gopi123@127.0.0.1:5435/database")

# df = pd.read_csv("/home/gopi/doker_airflow/hydro/threedays.csv")
# df.to_sql(
#     name='your_table_name',
#     con=engine,
#     if_exists='replace',  # or 'append'
#     index=False,
#     dtype={
#         'TIMESTAMP': sqlalchemy.types.TIMESTAMP(timezone=True)  # for TIMESTAMPTZ
#     }
# )


query = text("""
    SELECT "TIMESTAMP","UNIT","MW","MONITOR2","MONITOR1"
    FROM your_table_name
    WHERE "TIMESTAMP" BETWEEN '2025-06-16T00:00:00+05:30' AND '2025-06-18T23:59:59+05:30'
""")
with engine.connect() as conn:
    df = pd.read_sql(query, conn)
# print(df.keys())
print(df.head(50))

df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
df['interval'] = df['TIMESTAMP'].dt.floor('15min')

# 2. MW values for UNIT1–UNIT8
unit_mask = df['UNIT'].isin([f'UNIT{i}' for i in range(1, 9)])
df_unit = df[unit_mask][['interval', 'UNIT', 'MW']].copy()
df_unit['key'] = df_unit['UNIT']
df_unit['value'] = df_unit['MW']

# 3. monitor1 for reservoirs
res_mask = df['UNIT'].isin(['UpperReservoir', 'LowerReservoir'])

df_mon1 = df[res_mask][['interval', 'UNIT', 'MONITOR1']].copy()
df_mon1['key'] = df_mon1['UNIT'] + '_MONITOR1'
df_mon1['value'] = df_mon1['MONITOR1']

df_mon2 = df[res_mask][['interval', 'UNIT', 'MONITOR2']].copy()
df_mon2['key'] = df_mon2['UNIT'] + '_MONITOR2'
df_mon2['value'] = df_mon2['MONITOR2']

# 4. Combine all into a single DataFrame
combined_df = pd.concat([
    df_unit[['interval', 'key', 'value']],
    df_mon1[['interval', 'key', 'value']],
    df_mon2[['interval', 'key', 'value']]
])

# 5. Pivot the table
pivoted = combined_df.pivot_table(index='interval', columns='key', values='value', aggfunc='first').reset_index()

# Rename interval back to TIMESTAMP if needed
pivoted = pivoted.rename(columns={'interval': 'TIMESTAMP'})

# Show result
print(pivoted)
df = pivoted.copy()
df['UpperReservoir'] = np.where(
    (df['UpperReservoir_MONITOR1'] > 350) & (df['UpperReservoir_MONITOR2'] > 350),
    (df['UpperReservoir_MONITOR1'] + df['UpperReservoir_MONITOR2']) / 2,
    df[['UpperReservoir_MONITOR1', 'UpperReservoir_MONITOR2']].max(axis=1)
)

df['LowerReservoir'] = np.where(
    (df['LowerReservoir_MONITOR1'] > 100) & (df['LowerReservoir_MONITOR2'] > 100),
    (df['LowerReservoir_MONITOR1'] + df['LowerReservoir_MONITOR2']) / 2,
    df[['LowerReservoir_MONITOR1', 'LowerReservoir_MONITOR2']].max(axis=1)
)

unit_cols = [f'UNIT{i}' for i in range(1, 9)]

# Create a mask of values > 100
mask = abs(df[unit_cols]) > 200

# Replace values ≤100 with NaN and compute row-wise mean
df['Power'] = df[unit_cols].where(mask).mean(axis=1, skipna=True)
df.drop(columns=['UNIT1','UNIT2','UNIT3','UNIT4','UNIT5','UNIT6','UNIT7','UNIT8','LowerReservoir_MONITOR1',
                 'LowerReservoir_MONITOR2','UpperReservoir_MONITOR1','UpperReservoir_MONITOR2'],inplace=True)
df = df.interpolate(method='linear', limit_direction='both')

print(df)
df.to_csv("/home/gopi/doker_airflow/hydro/three_days.csv")