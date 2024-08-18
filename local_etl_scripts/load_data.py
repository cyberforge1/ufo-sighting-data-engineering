# local_etl_scripts/load_data.py

import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def connect_to_db():
    try:
        connection = psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB")
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

def load_data_to_db(csv_file_path):
    connection = connect_to_db()
    if connection is not None:
        cursor = connection.cursor()

        # Load data into pandas DataFrame
        df = pd.read_csv(csv_file_path)
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

        # Insert DataFrame records into PostgreSQL table
        for i, row in df.iterrows():
            cursor.execute(
                '''
                INSERT INTO ufo_sightings (datetime, city, state, country, shape, duration_seconds, 
                                           duration_hours_min, comments, date_posted, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                tuple(row)
            )
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Data loaded successfully from {csv_file_path}")
    else:
        print("Failed to load data due to connection error")
