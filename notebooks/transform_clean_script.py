import pandas as pd

file_path = './data/complete.csv'

chunk_size = 10000
chunks = []

for chunk in pd.read_csv(file_path, chunksize=chunk_size, on_bad_lines='skip'):
    expected_columns = 11
    chunk = chunk[chunk.apply(lambda x: len(x) == expected_columns, axis=1)]
    
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)

df.columns = [
    'datetime', 'city', 'state', 'country', 'shape',
    'duration (seconds)', 'duration (hours/min)', 'comments',
    'date posted', 'latitude', 'longitude'
]

df.head()


df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

df['date posted'] = pd.to_datetime(df['date posted'], errors='coerce').dt.date

df['duration (seconds)'] = pd.to_numeric(df['duration (seconds)'], errors='coerce')


df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

df['city'] = df['city'].str.strip().str.title()
df['state'] = df['state'].str.strip().str.upper()
df['country'] = df['country'].str.strip().str.upper()

df['duration (hours/min)'] = df['duration (hours/min)'].str.strip()

df.dropna(subset=['datetime', 'city', 'country'], inplace=True)

df.drop_duplicates(inplace=True)


df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]


df.rename(columns={
    'datetime': 'datetime',
    'city': 'city',
    'state': 'state',
    'country': 'country',
    'shape': 'shape',
    'duration (seconds)': 'duration_seconds',
    'duration (hours/min)': 'duration_hours_min',
    'comments': 'comments',
    'date posted': 'date_posted',
    'latitude': 'latitude',
    'longitude': 'longitude'
}, inplace=True)

df.head()

cleaned_file_path = './data/cleaned_ufo_sightings.csv'
df.to_csv(cleaned_file_path, index=False)

print(f"Cleaned data has been saved to {cleaned_file_path}")
