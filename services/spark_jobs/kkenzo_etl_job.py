from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os
import socket


# USER / INFRASTRUCTURE CONFIG


POSTGRES_HOST = "bd_postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "kkenzo_db"
POSTGRES_USER = "kkenzo"
POSTGRES_PASSWORD = "cz4yzZOe2YxWXZj1"

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

MINIO_ENDPOINT = "http://192.168.56.101:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minioP@szw0rD"
MINIO_BUCKET_PATH = "raw-data/data.parquet"


# SPARK ETL PIPELINE

def run_etl():
    print("Starting Spark ETL job")

    driver_hostname = socket.gethostname()

    spark = (
        SparkSession.builder
        .appName("kkenzo_spark_etl")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", driver_hostname)
        .config("spark.driver.bindAddress", "0.0.0.0")

        # ---- MinIO / S3A Configuration ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

    print("Spark session initialized successfully")

    # EXTRACT
    print("Loading raw dataset from MinIO")

    raw_df = spark.read.parquet(f"s3a://{MINIO_BUCKET_PATH}")
    print(f"Rows loaded: {raw_df.count()}")

    # TRANSFORM

    print("Applying transformations")

    cleaned_df = (
        raw_df
        .dropDuplicates()
        .filter(col("Name").isNotNull())
        .filter(col("Developers").isNotNull())
        .withColumn(
            "release_date_parsed",
            to_date(col("Release date"), "MMM d, yyyy")
        )
        .withColumn(
            "total_user_reviews",
            col("Positive") + col("Negative")
        )
    )

    print(f"Rows after cleaning: {cleaned_df.count()}")

    # LOAD
    print("Writing cleaned data to PostgreSQL")

    cleaned_df.write.jdbc(
        url=POSTGRES_JDBC_URL,
        table="clean_table",
        mode="overwrite",
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )

    print("ETL job completed successfully")
    spark.stop()


# ENTRY POINT

if __name__ == "__main__":
    run_etl()
