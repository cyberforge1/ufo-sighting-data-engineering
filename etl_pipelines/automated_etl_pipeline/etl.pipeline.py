# ./automated_etl_pipeline/etl_pipeline.py

import os
import pandas as pd
from io import BytesIO
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DateType
from dotenv import load_dotenv
import zipfile

def download_ufo_dataset():
    api = KaggleApi()
    api.authenticate()

    dataset_path = "nuforc/ufo-sightings"
    file_path = "./data/complete.csv"

    api.dataset_download_file(dataset_path, file_name='complete.csv', path="./data")

    if os.path.exists(file_path + ".zip"):
        with zipfile.ZipFile(file_path + ".zip", 'r') as zip_ref:
            zip_ref.extractall("./data")
        os.remove(file_path + ".zip")

    return file_path

def initial_clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    expected_columns = 11
    chunk = chunk[chunk.apply(lambda x: len(x) == expected_columns, axis=1)]

    chunk.columns = [
        'datetime', 'city', 'state', 'country', 'shape',
        'duration (seconds)', 'duration (hours/min)', 'comments',
        'date posted', 'latitude', 'longitude'
    ]

    chunk['datetime'] = pd.to_datetime(chunk['datetime'], errors='coerce')
    chunk['date posted'] = pd.to_datetime(chunk['date posted'], errors='coerce').dt.date
    chunk['duration (seconds)'] = pd.to_numeric(chunk['duration (seconds)'], errors='coerce')
    chunk['latitude'] = pd.to_numeric(chunk['latitude'], errors='coerce')
    chunk['longitude'] = pd.to_numeric(chunk['longitude'], errors='coerce')

    chunk['city'] = chunk['city'].str.strip().str.title()
    chunk['state'] = chunk['state'].str.strip().str.upper()
    chunk['country'] = chunk['country'].str.strip().str.upper()

    chunk['duration (hours/min)'] = chunk['duration (hours/min)'].str.strip()
    chunk.dropna(subset=['datetime', 'city', 'country'], inplace=True)
    chunk.drop_duplicates(inplace=True)
    chunk = chunk[(chunk['latitude'].between(-90, 90)) & (chunk['longitude'].between(-180, 180))]

    chunk.rename(columns={
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

    return chunk

def pandas_to_spark(spark, pandas_df, schema):
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    return spark_df

def extract_and_transform(file_path):
    spark = SparkSession.builder.appName("UFO_Sightings_ETL").getOrCreate()

    schema = StructType([
        StructField("datetime", TimestampType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("shape", StringType(), True),
        StructField("duration_seconds", FloatType(), True),
        StructField("duration_hours_min", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("date_posted", DateType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True)
    ])

    final_spark_df = spark.createDataFrame([], schema=schema)

    for chunk in pd.read_csv(file_path, chunksize=10000, on_bad_lines='skip'):
        cleaned_chunk = initial_clean_chunk(chunk)
        spark_chunk = pandas_to_spark(spark, cleaned_chunk, schema)
        final_spark_df = final_spark_df.union(spark_chunk)

    return final_spark_df

def load_data_to_db(df):
    load_dotenv()

    jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url=jdbc_url, table="ufo_sightings", mode="append", properties=connection_properties)
    print("Data loaded successfully into the PostgreSQL database")

def main():
    file_path = download_ufo_dataset()

    spark_df = extract_and_transform(file_path)

    load_data_to_db(spark_df)

if __name__ == "__main__":
    main()
