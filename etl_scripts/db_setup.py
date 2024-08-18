# etl_scripts/db_setup.py

import os
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
        print("Connected to the database")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

def create_table():
    connection = connect_to_db()
    if connection is not None:
        cursor = connection.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS ufo_sightings (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            city VARCHAR(100),
            state VARCHAR(50),
            country VARCHAR(50),
            shape VARCHAR(50),
            duration_seconds FLOAT,
            duration_hours_min VARCHAR(50),
            comments TEXT,
            date_posted DATE,
            latitude FLOAT,
            longitude FLOAT
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
        print("Table created successfully")
    else:
        print("Failed to create table due to connection error")
