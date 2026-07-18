import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone


s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

BUCKET_NAME = "api-etl-project"
RAW_PREFIX = "raw/api/stocks/"
SECRET_NAME = "alpha-vantage-api-key"


def get_api_key():
    secret_response = secrets_client.get_secret_value(
        SecretId=SECRET_NAME
    )

    secret_string = secret_response["SecretString"]
    secret_dict = json.loads(secret_string)

    return secret_dict["ALPHA_VANTAGE_API_KEY"]


def lambda_handler(event, context):
    print("Starting Alpha Vantage API ingestion job")

    try:
        api_key = get_api_key()

        query_params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": "IBM",
            "apikey": api_key
        }

        api_url = "https://www.alphavantage.co/query?" + urllib.parse.urlencode(query_params)

        print("Calling Alpha Vantage API")

        with urllib.request.urlopen(api_url) as response:
            api_data = json.loads(response.read().decode("utf-8"))

        if "Error Message" in api_data:
            raise Exception(f"API returned error: {api_data['Error Message']}")

        if "Note" in api_data:
            raise Exception(f"API limit message: {api_data['Note']}")

        ingestion_time = datetime.now(timezone.utc)

        file_name = f"alpha_vantage_ibm_daily_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json"
        s3_key = RAW_PREFIX + file_name

        output_payload = {
            "ingestion_timestamp": ingestion_time.isoformat(),
            "source_api": "Alpha Vantage",
            "symbol": "IBM",
            "function": "TIME_SERIES_DAILY",
            "s3_file_name": file_name,
            "data": api_data
        }

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(output_payload, indent=2),
            ContentType="application/json"
        )

        print(f"Successfully uploaded raw API data to s3://{BUCKET_NAME}/{s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Alpha Vantage API data ingested successfully",
                "s3_path": f"s3://{BUCKET_NAME}/{s3_key}",
                "symbol": "IBM"
            })
        }

    except Exception as error:
        print(f"Error during API ingestion: {str(error)}")

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "API ingestion failed",
                "error": str(error)
            })
        }
