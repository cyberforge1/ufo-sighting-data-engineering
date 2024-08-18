# pyspark_etl_scripts/pyspark_etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, trim, upper, initcap
from pyspark.sql.types import FloatType, StringType

def clean_ufo_data(file_path, cleaned_file_path):
    jdbc_driver_path = "/Users/softdev/Desktop/github-projects/where-are-the-aliens/postgresql-42.7.3.jar"

    spark = SparkSession.builder \
        .appName("UFO ETL") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


    df = spark.read.option("header", "true").option("mode", "DROPMALFORMED").csv(file_path)

    expected_columns = [
        'datetime', 'city', 'state', 'country', 'shape',
        'duration (seconds)', 'duration (hours/min)', 'comments',
        'date posted', 'latitude', 'longitude'
    ]
    if len(df.columns) != len(expected_columns):
        df = df.select(df.columns[:len(expected_columns)])

    df = df.toDF(
        'datetime', 'city', 'state', 'country', 'shape',
        'duration_seconds', 'duration_hours_min', 'comments',
        'date_posted', 'latitude', 'longitude'
    )

    df = df.withColumn("datetime", to_timestamp(col("datetime"), "MM/dd/yyyy HH:mm")) \
           .withColumn("date_posted", to_date(col("date_posted"), "MM/dd/yyyy")) \
           .withColumn("duration_seconds", col("duration_seconds").cast(FloatType())) \
           .withColumn("latitude", col("latitude").cast(FloatType())) \
           .withColumn("longitude", col("longitude").cast(FloatType()))

    df = df.withColumn("city", initcap(trim(col("city")))) \
           .withColumn("state", upper(trim(col("state")))) \
           .withColumn("country", upper(trim(col("country")))) \
           .withColumn("duration_hours_min", trim(col("duration_hours_min")))

    df = df.na.drop(subset=["datetime", "city", "country"])

    df = df.dropDuplicates()

    df = df.filter((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)))

    pandas_df = df.toPandas()

    final_csv_path = cleaned_file_path + ".csv"
    pandas_df.to_csv(final_csv_path, index=False)

    print(f"Cleaned data has been saved to {final_csv_path}")

    spark.stop()
