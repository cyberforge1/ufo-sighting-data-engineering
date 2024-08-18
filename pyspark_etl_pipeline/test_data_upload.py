# pyspark_etl_scripts/test_data_upload.py

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def test_data_upload():
    try:
        connection = psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB")
        )
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM ufo_sightings;")
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            print(f"Data upload successful: {row_count} rows found in the ufo_sightings table.")
        else:
            print("Data upload failed: No rows found in the ufo_sightings table.")


        cursor.execute("SELECT * FROM ufo_sightings LIMIT 5;")
        rows = cursor.fetchall()
        print("Sample data from ufo_sightings table:")
        for row in rows:
            print(row)

        cursor.close()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

if __name__ == "__main__":
    test_data_upload()
