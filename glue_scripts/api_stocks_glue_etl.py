import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    explode,
    input_file_name,
    current_timestamp,
    to_date,
    to_json,
    from_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType
)


# -------------------------------------------------------
# 1. Read Glue job arguments
# -------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

bucket = args["BUCKET_NAME"]


# -------------------------------------------------------
# 2. Define S3 paths
# -------------------------------------------------------

raw_path = f"s3://{bucket}/raw/api/stocks/"
processed_path = f"s3://{bucket}/processed/stocks/"
quarantine_path = f"s3://{bucket}/quarantine/stocks/"


# -------------------------------------------------------
# 3. Start Spark / Glue session
# -------------------------------------------------------

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


# -------------------------------------------------------
# 4. Define schema for Alpha Vantage daily price data
# -------------------------------------------------------

price_schema = StructType([
    StructField("1. open", StringType(), True),
    StructField("2. high", StringType(), True),
    StructField("3. low", StringType(), True),
    StructField("4. close", StringType(), True),
    StructField("5. volume", StringType(), True)
])

time_series_schema = MapType(StringType(), price_schema)


# -------------------------------------------------------
# 5. Read raw JSON files from S3
# -------------------------------------------------------

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(raw_path)
    .withColumn("source_file_path", input_file_name())
)


# -------------------------------------------------------
# 6. Extract metadata fields and convert time series to map
# -------------------------------------------------------

metadata_df = raw_df.select(
    col("ingestion_timestamp"),
    col("source_api"),
    col("symbol"),
    col("function"),
    col("s3_file_name"),
    col("source_file_path"),
    from_json(
        to_json(col("data.`Time Series (Daily)`")),
        time_series_schema
    ).alias("time_series")
)


# -------------------------------------------------------
# 7. Flatten daily stock time series
# -------------------------------------------------------

flattened_df = (
    metadata_df
    .select(
        col("ingestion_timestamp"),
        col("source_api"),
        col("symbol"),
        col("function"),
        col("s3_file_name"),
        col("source_file_path"),
        explode(col("time_series")).alias("trade_date", "price_data")
    )
)


# -------------------------------------------------------
# 8. Select and rename columns
# -------------------------------------------------------

stock_prices_df = flattened_df.select(
    col("symbol"),
    to_date(col("trade_date"), "yyyy-MM-dd").alias("trade_date"),
    col("price_data.`1. open`").cast("double").alias("open_price"),
    col("price_data.`2. high`").cast("double").alias("high_price"),
    col("price_data.`3. low`").cast("double").alias("low_price"),
    col("price_data.`4. close`").cast("double").alias("close_price"),
    col("price_data.`5. volume`").cast("long").alias("volume"),
    col("ingestion_timestamp"),
    col("source_api"),
    col("function"),
    col("s3_file_name"),
    current_timestamp().alias("etl_processed_at")
)


# -------------------------------------------------------
# 9. Data quality checks
# -------------------------------------------------------

valid_df = stock_prices_df.filter(
    col("symbol").isNotNull()
    & col("trade_date").isNotNull()
    & col("open_price").isNotNull()
    & col("high_price").isNotNull()
    & col("low_price").isNotNull()
    & col("close_price").isNotNull()
    & col("volume").isNotNull()
)

invalid_df = stock_prices_df.filter(
    col("symbol").isNull()
    | col("trade_date").isNull()
    | col("open_price").isNull()
    | col("high_price").isNull()
    | col("low_price").isNull()
    | col("close_price").isNull()
    | col("volume").isNull()
)


# -------------------------------------------------------
# 10. Write invalid records to quarantine
# -------------------------------------------------------

if not invalid_df.rdd.isEmpty():
    (
        invalid_df
        .write
        .mode("append")
        .parquet(quarantine_path)
    )


# -------------------------------------------------------
# 11. Write valid records to processed S3 as Parquet
# -------------------------------------------------------

if not valid_df.rdd.isEmpty():
    (
        valid_df
        .write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(processed_path)
    )


print("API stock data Glue ETL completed successfully.")