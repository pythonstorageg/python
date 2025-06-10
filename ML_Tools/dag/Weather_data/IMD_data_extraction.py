import numpy as np
from datetime import datetime
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Load the GRD binary data (366 days × 31 lat × 31 lon)
file_path = "/home/gopi/doker_airflow/IMD/Maxtemp_MaxT_2024.GRD"
data = np.fromfile(file_path, dtype=np.float32).reshape((366, 31, 31))

# Define coordinate grids
latitudes = np.arange(7.5, 38.0, 1.0)    # 31 lat values
longitudes = np.arange(67.5, 98.0, 1.0)  # 31 lon values
start_date = datetime(2024, 1, 1)        # Replace with actual year

# Create records for each (day, lat, lon)
records = []
for day_idx in range(366):
    current_date = start_date + timedelta(days=day_idx)
    for i, lat in enumerate(latitudes):
        for j, lon in enumerate(longitudes):
            temp = data[day_idx, i, j]
            records.append((current_date.strftime("%Y-%m-%d"), lat, lon, temp))

# Convert to DataFrame
df = pd.DataFrame(records, columns=["date", "lat", "lon", "temperature"])
dist = np.sqrt((df['lat'] - 14.815438)**2 + (df['lon'] - 77.210792)**2)
nearest_idx = dist.idxmin()
nearest_lat = df.loc[nearest_idx, 'lat']
nearest_lon = df.loc[nearest_idx, 'lon']
nearest_rows = df[(df['lat'] == nearest_lat) & (df['lon'] == nearest_lon)]
print(nearest_rows)
# Optional: Save to CSV
# df.to_csv("imd_temperature_2016.csv", index=False)

# Preview
# print(df.head())
