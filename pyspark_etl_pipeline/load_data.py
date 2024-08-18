# pyspark_etl_scripts/load_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import FloatType
import os
from dotenv import load_dotenv

load_dotenv()

def load_data_to_db(csv_file_path):
    jdbc_driver_path = "/Users/softdev/Desktop/github-projects/where-are-the-aliens/postgresql-42.7.3.jar"

    spark = SparkSession.builder \
        .appName("Load UFO Data") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    df = spark.read.option("header", "true").csv(csv_file_path)

    # Ensure datetime, duration_seconds, latitude, and longitude are cast to the correct types
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("date_posted", to_timestamp(col("date_posted"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("duration_seconds", col("duration_seconds").cast(FloatType()))
    df = df.withColumn("latitude", col("latitude").cast(FloatType()))
    df = df.withColumn("longitude", col("longitude").cast(FloatType()))

    db_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
        table="ufo_sightings",
        mode="append",
        properties=db_properties
    )

    print(f"Data loaded successfully from {csv_file_path}")

    spark.stop()

