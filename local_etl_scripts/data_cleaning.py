# local_etl_scripts/data_cleaning.py

import pandas as pd

def clean_ufo_data(file_path, cleaned_file_path):
    # Step 1: Read the file in chunks and preprocess
    chunk_size = 10000  # Define a chunk size
    chunks = []

    # Loop through chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, on_bad_lines='skip'):
        # Step 2: Remove rows with an inconsistent number of columns
        expected_columns = 11
        chunk = chunk[chunk.apply(lambda x: len(x) == expected_columns, axis=1)]
        chunks.append(chunk)

    # Concatenate all chunks back into a single DataFrame
    df = pd.concat(chunks, ignore_index=True)

    # Step 3: Rename the columns as per the schema
    df.columns = [
        'datetime', 'city', 'state', 'country', 'shape',
        'duration (seconds)', 'duration (hours/min)', 'comments',
        'date posted', 'latitude', 'longitude'
    ]

    # Convert 'datetime' to TIMESTAMP
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    # Convert 'date posted' to DATE
    df['date posted'] = pd.to_datetime(df['date posted'], errors='coerce').dt.date

    # Convert 'duration (seconds)' to FLOAT
    df['duration (seconds)'] = pd.to_numeric(df['duration (seconds)'], errors='coerce')

    # Convert 'latitude' and 'longitude' to FLOAT
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    # Clean String Data
    df['city'] = df['city'].str.strip().str.title()
    df['state'] = df['state'].str.strip().str.upper()
    df['country'] = df['country'].str.strip().str.upper()

    # Keep 'duration (hours/min)' as a string
    df['duration (hours/min)'] = df['duration (hours/min)'].str.strip()

    # Handle Missing Values
    df.dropna(subset=['datetime', 'city', 'country'], inplace=True)

    # Remove Duplicates
    df.drop_duplicates(inplace=True)

    # Validate Latitude and Longitude
    df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]

    # Rename Columns to Match Database Schema
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

    # Save Cleaned Data to a New CSV File
    df.to_csv(cleaned_file_path, index=False)

    print(f"Cleaned data has been saved to {cleaned_file_path}")
