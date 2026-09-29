```python
"""
Lambda: JSON Reference Data → Silver Layer (Parquet)
────────────────────────────────────────────────────

Triggered by S3 event when new JSON lands in the Bronze bucket
under the reference_data prefix.

Bronze Bucket:
    youtube-datapiepline-ap-south2

Bronze Reference Path:
    youtube/raw_statistics_reference_data/

Silver Bucket:
    youtube-datapiepline-silver-ap-south2

Silver Reference Path:
    youtube/raw_statistics_reference_data/

Example input:
    s3://youtube-datapiepline-ap-south2/
    youtube/raw_statistics_reference_data/region=ca/
    CA_category_id.json

Example output:
    s3://youtube-datapiepline-silver-ap-south2/
    youtube/raw_statistics_reference_data/region=ca/
    *.parquet


Environment Variables:
    GLUE_DB_SILVER              — Glue catalog database name
    GLUE_TABLE_REFERENCE        — Glue catalog table name
    SNS_ALERT_TOPIC_ARN         — SNS topic for alerts (optional)
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd


# ── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger()

logger.setLevel(logging.INFO)


# ── Bucket Configuration ─────────────────────────────────────────────────────

# YOUR BRONZE / RAW BUCKET
BRONZE_BUCKET = "youtube-datapiepline-ap-south2"

# YOUR SILVER BUCKET
SILVER_BUCKET = "youtube-datapiepline-silver-ap-south2"


# ── Glue Configuration ───────────────────────────────────────────────────────

GLUE_DB = os.environ.get(
    "GLUE_DB_SILVER",
    "yt_pipeline_silver_dev"
)

GLUE_TABLE = os.environ.get(
    "GLUE_TABLE_REFERENCE",
    "clean_reference_data"
)


# ── SNS Configuration ────────────────────────────────────────────────────────

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
)


# ── S3 Paths ──────────────────────────────────────────────────────────────────

# Bronze reference-data location
BRONZE_PATH = (
    f"s3://{BRONZE_BUCKET}/"
    f"youtube/raw_statistics_reference_data/"
)


# Silver reference-data location
SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/"
    f"youtube/raw_statistics_reference_data/"
)


# ── AWS Clients ──────────────────────────────────────────────────────────────

s3_client = boto3.client("s3")

sns_client = boto3.client("sns")


# ─────────────────────────────────────────────────────────────────────────────
# Read JSON from S3
# ─────────────────────────────────────────────────────────────────────────────

def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read raw JSON from S3 using boto3 instead of awswrangler.

    The YouTube/Kaggle category JSON has mixed types:

        {
            "kind": "...",
            "etag": "...",
            "items": [...]
        }

    Because of the nested items array, using boto3 + json.loads()
    is safer than directly using wr.s3.read_json().
    """

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    content = response["Body"].read().decode("utf-8")

    return json.loads(content)


# ─────────────────────────────────────────────────────────────────────────────
# Validate Category Data
# ─────────────────────────────────────────────────────────────────────────────

def validate_category_data(
    df: pd.DataFrame
) -> pd.DataFrame:

    """
    Validate and clean category reference data.

    Returns cleaned DataFrame or raises ValueError.
    """

    # Check empty DataFrame
    if df.empty:

        raise ValueError(
            "Empty DataFrame — no category items found"
        )


    # Expected columns after json_normalize()
    required_cols = {
        "id",
        "snippet.title"
    }


    actual_cols = set(df.columns)


    missing = required_cols - actual_cols


    if missing:

        logger.warning(
            f"Missing expected columns: {missing}. "
            f"Available columns: {actual_cols}"
        )


    # ── Remove duplicate category records ─────────────────────────────────

    before = len(df)


    if "id" in df.columns:

        df = df.drop_duplicates(
            subset=["id"],
            keep="last"
        )


    after = len(df)


    if before != after:

        logger.info(
            f"Removed {before - after} duplicate categories"
        )


    return df


# ─────────────────────────────────────────────────────────────────────────────
# SNS Alert
# ─────────────────────────────────────────────────────────────────────────────

def send_alert(
    subject: str,
    message: str
):

    """
    Send SNS alert if SNS topic is configured.
    """

    if SNS_TOPIC:

        sns_client.publish(

            TopicArn=SNS_TOPIC,

            Subject=subject[:100],

            Message=message

        )

        logger.info(
            "SNS alert sent successfully"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Extract Region
# ─────────────────────────────────────────────────────────────────────────────

def extract_region(key: str) -> str:

    """
    Extract region from S3 key.

    Example:

    youtube/raw_statistics_reference_data/
    region=ca/
    CA_category_id.json

    Returns:

        ca
    """

    for part in key.split("/"):

        if part.startswith("region="):

            return part.split("=", 1)[1]


    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):

    """
    Process S3 event for new JSON reference files.
    """

    logger.info(
        "Lambda execution started"
    )


    processed = []

    errors = []


    # ─────────────────────────────────────────────────────────────────────
    # Get S3 event records
    # ─────────────────────────────────────────────────────────────────────

    records = event.get(
        "Records",
        []
    )


    if not records:

        # Could be invoked directly by Step Functions
        records = (
            [event]
            if "s3" in event
            else []
        )


    # ─────────────────────────────────────────────────────────────────────
    # Process each record
    # ─────────────────────────────────────────────────────────────────────

    for record in records:

        key = "unknown"


        try:

            # ── Get S3 information ───────────────────────────────────────

            s3_info = record["s3"]


            bucket = s3_info["bucket"]["name"]


            key = unquote_plus(
                s3_info["object"]["key"]
            )


            logger.info(
                f"Processing: s3://{bucket}/{key}"
            )


            # ── Validate bucket ──────────────────────────────────────────

            if bucket != BRONZE_BUCKET:

                raise ValueError(
                    f"Unexpected source bucket: {bucket}. "
                    f"Expected: {BRONZE_BUCKET}"
                )


            # ── Validate prefix ─────────────────────────────────────────

            if not key.startswith(
                "youtube/raw_statistics_reference_data/"
            ):

                logger.info(
                    f"Skipping file outside reference-data path: {key}"
                )

                continue


            # ── Validate file extension ──────────────────────────────────

            if not key.lower().endswith(".json"):

                logger.info(
                    f"Skipping non-JSON file: {key}"
                )

                continue


            # ─────────────────────────────────────────────────────────────
            # Read raw JSON
            # ─────────────────────────────────────────────────────────────

            raw_data = read_json_from_s3(
                bucket,
                key
            )


            # ─────────────────────────────────────────────────────────────
            # Extract items
            # ─────────────────────────────────────────────────────────────

            if (
                "items" in raw_data
                and isinstance(
                    raw_data["items"],
                    list
                )
            ):

                df = pd.json_normalize(
                    raw_data["items"]
                )

            else:

                # Fallback
                df = pd.json_normalize(
                    raw_data
                )


            logger.info(
                f"Raw shape: {df.shape}"
            )


            # ─────────────────────────────────────────────────────────────
            # Validate data
            # ─────────────────────────────────────────────────────────────

            df = validate_category_data(
                df
            )


            # ─────────────────────────────────────────────────────────────
            # Add metadata columns
            # ─────────────────────────────────────────────────────────────

            df["_ingestion_timestamp"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )


            df["_source_file"] = key


            # ─────────────────────────────────────────────────────────────
            # Extract region
            # ─────────────────────────────────────────────────────────────

            region = extract_region(
                key
            )


            df["region"] = region


            logger.info(
                f"Clean shape: {df.shape}"
            )


            logger.info(
                f"Region: {region}"
            )


            # ─────────────────────────────────────────────────────────────
            # Write to Silver as Parquet
            # ─────────────────────────────────────────────────────────────

            wr_response = wr.s3.to_parquet(

                df=df,

                path=SILVER_PATH,

                dataset=True,

                database=GLUE_DB,

                table=GLUE_TABLE,

                partition_cols=[
                    "region"
                ],

                mode="overwrite_partitions",

                schema_evolution=True,

                index=False

            )


            logger.info(
                f"Written to Silver: {SILVER_PATH}"
            )


            # ─────────────────────────────────────────────────────────────
            # Success information
            # ─────────────────────────────────────────────────────────────

            processed.append({

                "source_bucket": bucket,

                "key": key,

                "region": region,

                "rows": len(df),

                "silver_path": SILVER_PATH

            })


        # ─────────────────────────────────────────────────────────────────
        # Error handling
        # ─────────────────────────────────────────────────────────────────

        except Exception as e:

            logger.error(
                f"Error processing record: {str(e)}",
                exc_info=True
            )


            errors.append({

                "key": key,

                "error": str(e)

            })


    # ─────────────────────────────────────────────────────────────────────
    # Send SNS alert if errors occurred
    # ─────────────────────────────────────────────────────────────────────

    if errors:

        send_alert(

            subject=(
                "[YT Pipeline] "
                "Silver reference transform failed"
            ),

            message=json.dumps(
                errors,
                indent=2
            )

        )


    # ─────────────────────────────────────────────────────────────────────
    # Lambda response
    # ─────────────────────────────────────────────────────────────────────

    response = {

        "statusCode": 200,

        "processed": processed,

        "errors": errors

    }


    logger.info(
        f"Lambda response: {json.dumps(response)}"
    )


    return response
```
