# Authenticated API Ingestion ETL Pipeline with AWS Lambda, S3, Glue, and Athena

## Project Overview

This project demonstrates an end-to-end AWS data engineering pipeline that ingests stock market data from a real authenticated API, stores the raw response in Amazon S3, transforms nested JSON data using AWS Glue PySpark, and makes the processed data queryable using Amazon Athena.

The pipeline uses Alpha Vantage as the external API source and ingests daily stock price data for IBM. The raw API response is stored as JSON, then converted into a clean, analytics-ready Parquet dataset.

## Architecture

```text
Alpha Vantage API
        ↓
AWS Lambda
        ↓
Amazon S3 Raw Zone
        ↓
AWS Glue ETL
        ↓
Amazon S3 Processed Zone
        ↓
AWS Glue Crawler
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
```

## AWS Services Used

- **AWS Lambda** – Calls the Alpha Vantage API and stores the raw JSON response in S3.
- **AWS Secrets Manager** – Stores the Alpha Vantage API key securely.
- **Amazon S3** – Stores raw API responses and processed Parquet output.
- **AWS Glue** – Runs the PySpark ETL job to flatten and transform nested JSON.
- **AWS Glue Crawler** – Crawls the processed Parquet data and creates a table in the Glue Data Catalog.
- **AWS Glue Data Catalog** – Stores table metadata for Athena.
- **Amazon Athena** – Queries the processed dataset using SQL.
- **Amazon CloudWatch** – Captures Lambda and Glue logs for monitoring and debugging.
- **IAM** – Provides permissions for Lambda and Glue to access AWS services.

## Project Folder Structure

```text
aws-api-lambda-glue-athena-etl-project/
│
├── api_response/
│   └── sample_alpha_vantage_ibm_response.json
│
├── lambda_functions/
│   └── lambda_function.py
│
├── glue_scripts/
│   └── api_stocks_glue_etl.py
│
├── athena_queries/
│   └── validation_queries.sql
│
├── screenshots/
│   ├── 01_lambda_success_cloudwatch.png
│   ├── 02_s3_raw_json.png
│   ├── 03_glue_job_success.png
│   ├── 04_s3_processed_parquet.png
│   ├── 05_crawler_success.png
│   ├── 06_glue_catalog_table.png
│   ├── 07_athena_preview_query.png
│   └── 08_athena_analytics_query.png
│
└── README.md
```

## Data Source

The pipeline uses the Alpha Vantage API to pull daily time series stock data.

```text
Function: TIME_SERIES_DAILY
Symbol: IBM
Source API: Alpha Vantage
```

The Lambda function retrieves the API key securely from AWS Secrets Manager instead of hardcoding it in the code.

## S3 Folder Structure

```text
s3://api-etl-project/
│
├── raw/
│   └── api/
│       └── stocks/
│
├── processed/
│   └── stocks/
│
├── quarantine/
│   └── stocks/
│
├── scripts/
│
└── logs/
```

## Pipeline Steps

### 1. API Key Storage

The Alpha Vantage API key is stored in AWS Secrets Manager.

```text
Secret name: alpha-vantage-api-key
Secret key: ALPHA_VANTAGE_API_KEY
```

This keeps the API key secure and avoids exposing it inside the Lambda code.

### 2. Lambda API Ingestion

AWS Lambda calls the Alpha Vantage API and stores the raw API response in S3.

Raw S3 path:

```text
s3://api-etl-project/raw/api/stocks/
```

The raw JSON file includes metadata such as ingestion timestamp, source API, stock symbol, API function, raw S3 file name, and the full API response data.

Example output file:

```text
alpha_vantage_ibm_daily_YYYYMMDD_HHMMSS.json
```

### 3. Raw JSON Storage

The raw API response is stored without transformation. This is useful for auditing, replaying, and debugging the original API response.

### 4. Glue ETL Transformation

AWS Glue reads the raw nested JSON file from S3 and transforms it into a clean table format.

The Glue job performs the following transformations:

- Reads multi-line JSON from S3.
- Extracts metadata fields.
- Converts nested stock time series data into rows.
- Uses `explode()` to flatten daily stock prices.
- Converts price fields from string to numeric values.
- Converts trade date from string to date type.
- Adds an ETL processing timestamp.
- Separates valid and invalid records.
- Writes valid records to the processed S3 zone.
- Writes invalid records to the quarantine S3 zone.

Processed output path:

```text
s3://api-etl-project/processed/stocks/
```

The processed data is written as Parquet and partitioned by stock symbol.

```text
processed/stocks/symbol=IBM/
```

### 5. Data Quality Checks

The Glue job validates important fields before writing data to the processed layer.

Required fields:

- symbol
- trade_date
- open_price
- high_price
- low_price
- close_price
- volume

Valid records are written to:

```text
s3://api-etl-project/processed/stocks/
```

Invalid records are written to:

```text
s3://api-etl-project/quarantine/stocks/
```

### 6. Glue Crawler and Data Catalog

A Glue crawler scans the processed Parquet files and creates a table in the Glue Data Catalog.

```text
Glue database: api_etl_project_db
Athena table: api_stocks
```

### 7. Athena SQL Validation

Preview data:

```sql
SELECT *
FROM api_stocks
LIMIT 10;
```

Count total rows:

```sql
SELECT COUNT(*) AS total_rows
FROM api_stocks;
```

View latest stock prices:

```sql
SELECT 
  symbol,
  trade_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume
FROM api_stocks
ORDER BY trade_date DESC
LIMIT 10;
```

Analytics summary:

```sql
SELECT
  symbol,
  MIN(trade_date) AS earliest_trade_date,
  MAX(trade_date) AS latest_trade_date,
  ROUND(AVG(close_price), 2) AS avg_close_price,
  MAX(high_price) AS highest_price,
  MIN(low_price) AS lowest_price,
  SUM(volume) AS total_volume
FROM api_stocks
GROUP BY symbol;
```

## Final Athena Table Columns

```text
symbol
trade_date
open_price
high_price
low_price
close_price
volume
ingestion_timestamp
source_api
function
s3_file_name
etl_processed_at
```

## Monitoring and Logging

CloudWatch is used to monitor Lambda and Glue execution.

Lambda logs confirm:

- Lambda started successfully.
- Alpha Vantage API was called.
- Raw API data was uploaded to S3.
- Lambda completed without error.

Example CloudWatch log messages:

```text
Starting Alpha Vantage API ingestion job
Calling Alpha Vantage API
Successfully uploaded raw API data to s3://api-etl-project/raw/api/stocks/...
```

Glue job logs are also available in CloudWatch and help debug ETL failures or schema issues.

## Key Learning Outcomes

This project demonstrates:

- Building an authenticated API ingestion pipeline on AWS.
- Secure API key handling using AWS Secrets Manager.
- Serverless ingestion using AWS Lambda.
- Raw and processed data lake design using Amazon S3.
- Transforming nested JSON using AWS Glue PySpark.
- Handling schema issues in semi-structured API data.
- Writing analytics-ready Parquet files.
- Creating Athena tables using Glue Crawlers.
- Validating pipeline output using SQL.
- Using CloudWatch logs for monitoring and debugging.

## Why Parquet Was Used

The raw API response is stored as JSON because it preserves the original source format.

The processed output is stored as Parquet because Parquet is better for analytics workloads. It is columnar, compressed, schema-friendly, and more efficient for Athena queries.

```text
Raw Zone: JSON
Processed Zone: Parquet
Query Layer: Athena
```

## Project Status

Completed:

- API key stored in Secrets Manager
- Lambda ingestion completed
- Raw JSON written to S3
- Glue ETL job completed successfully
- Processed Parquet data written to S3
- Glue crawler created Athena table
- Athena queries validated successfully

## Future Enhancements

- Add multiple stock symbols such as AAPL, MSFT, GOOGL, and AMZN.
- Schedule Lambda using Amazon EventBridge.
- Add incremental processing logic.
- Add data quality metrics and alerts.
- Create a dashboard using Amazon QuickSight or Power BI.
- Add CI/CD deployment using Terraform or AWS CDK.
- Store processed data in Apache Iceberg format for better upserts and versioning.
