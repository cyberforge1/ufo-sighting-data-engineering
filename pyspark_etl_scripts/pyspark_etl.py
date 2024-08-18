# pyspark_etl_scripts/pyspark_etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, trim, upper, initcap
import os

def clean_ufo_data(file_path, cleaned_file_path):
    # Path to the PostgreSQL JDBC driver
    jdbc_driver_path = "/Users/softdev/Desktop/github-projects/where-are-the-aliens/postgresql-42.7.3.jar"

    # Initialize Spark session with the JDBC driver in the classpath
    spark = SparkSession.builder \
        .appName("UFO ETL") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .getOrCreate()

    # Step 1: Read the CSV file into a DataFrame
    df = spark.read.option("header", "true").csv(file_path)

    # Step 2: Remove rows with an inconsistent number of columns (handled automatically by PySpark)
    df = df.filter(col(df.columns[0]).isNotNull())  # Ensure the first column is not null

    # Continue with other steps as before
    df = df.toDF(
        'datetime', 'city', 'state', 'country', 'shape',
        'duration_seconds', 'duration_hours_min', 'comments',
        'date_posted', 'latitude', 'longitude'
    )

    # Step 4: Convert data types
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("date_posted", to_date(col("date_posted"), "yyyy-MM-dd")) \
           .withColumn("duration_seconds", col("duration_seconds").cast("float")) \
           .withColumn("latitude", col("latitude").cast("float")) \
           .withColumn("longitude", col("longitude").cast("float"))

    # Step 5: Clean string data
    df = df.withColumn("city", initcap(trim(col("city")))) \
           .withColumn("state", upper(trim(col("state")))) \
           .withColumn("country", upper(trim(col("country"))))

    # Handle Missing Values
    df = df.na.drop(subset=["datetime", "city", "country"])

    # Remove Duplicates
    df = df.dropDuplicates()

    # Validate Latitude and Longitude
    df = df.filter((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)))

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Step 6: Write the Pandas DataFrame directly to a single CSV file
    final_csv_path = cleaned_file_path + ".csv"
    pandas_df.to_csv(final_csv_path, index=False)

    print(f"Cleaned data has been saved to {final_csv_path}")

    # Stop Spark session
    spark.stop()
