# pyspark_etl_scripts/load_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv

load_dotenv()

def load_data_to_db(csv_file_path):
    # Set the path to the PostgreSQL JDBC driver
    jdbc_driver_path = "/Users/softdev/Desktop/github-projects/where-are-the-aliens/postgresql-42.7.3.jar"

    # Initialize Spark session with the JDBC driver in the classpath
    spark = SparkSession.builder \
        .appName("Load UFO Data") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    # Step 1: Read the cleaned CSV file into a DataFrame
    df = spark.read.option("header", "true").csv(csv_file_path)

    # Step 2: Define the PostgreSQL JDBC connection properties
    db_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    # Step 3: Load the DataFrame into the PostgreSQL table
    df.write.jdbc(
        url=f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
        table="ufo_sightings",
        mode="append",
        properties=db_properties
    )

    print(f"Data loaded successfully from {csv_file_path}")

    # Stop Spark session
    spark.stop()
