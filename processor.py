#!/usr/bin/env python3
import json
import io
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
# logging - importing logging so processing events get written to the same log file as app.py
import logging

from baseline import BaselineManager
from detector import AnomalyDetector

s3 = boto3.client("s3")

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]  # students configure this

# logging - using the same logger as app.py so everything goes to one file
logger = logging.getLogger(__name__)

# logging - path to the local log file so we can sync it to S3
LOG_FILE_PATH = "/opt/anomaly-detection/app.log"


def sync_log_to_s3(bucket: str):
    # logging - syncing the local log file to S3 so we have a backup after every batch
    try:
        with open(LOG_FILE_PATH, "rb") as f:
            s3.put_object(
                Bucket=bucket,
                Key="logs/app.log",
                Body=f.read(),
                ContentType="text/plain"
            )
        logger.info("log file synced to s3://{}/logs/app.log".format(bucket))
    except Exception as e:
        print(f"[ERROR] failed to sync log file to S3: {e}")
        logger.error(f"failed to sync log file to S3: {e}")


def process_file(bucket: str, key: str):
    print(f"[INFO] processing: s3://{bucket}/{key}")
    # logging - log the start of each file processing so we have a record of every batch
    logger.info(f"started processing: s3://{bucket}/{key}")

    # 1. Download raw file
    # if we can't get the file there's nothing to do — bail out early
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        print(f"[INFO] loaded {len(df)} rows, columns: {list(df.columns)}")
        # logging - log how many rows and which columns came in
        logger.info(f"loaded {len(df)} rows from {key}, columns: {list(df.columns)}")
    except ClientError as e:
        print(f"[ERROR] couldn't download s3://{bucket}/{key}: {e}")
        # logging - log S3 download failures
        logger.error(f"couldn't download s3://{bucket}/{key}: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] failed to read CSV from s3://{bucket}/{key}: {e}")
        # logging - log CSV read failures
        logger.error(f"failed to read CSV from s3://{bucket}/{key}: {e}")
        return None

    # no point continuing if the file came back empty
    if df.empty:
        print(f"[ERROR] file s3://{bucket}/{key} is empty, skipping")
        # logging - log empty files so we can spot data pipeline issues
        logger.error(f"file s3://{bucket}/{key} is empty, skipping")
        return None

    # 2. Load current baseline
    # if baseline manager fails to init we can't score anything reliably
    try:
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()
    except Exception as e:
        print(f"[ERROR] failed to initialize baseline manager: {e}")
        # logging - log baseline init failures
        logger.error(f"failed to initialize baseline manager: {e}")
        return None

    # 3. Update baseline with values from this batch BEFORE scoring
    #    (use only non-null values for each channel)
    # wrapping per-column so one bad channel doesn't stop the others from updating
    for col in NUMERIC_COLS:
        if col in df.columns:
            try:
                clean_values = df[col].dropna().tolist()
                if clean_values:
                    baseline = baseline_mgr.update(baseline, col, clean_values)
            except Exception as e:
                print(f"[ERROR] failed to update baseline for column {col}: {e}")
                # logging - log per-column baseline update failures
                logger.error(f"failed to update baseline for column {col}: {e}")
                continue

    # 4. Run detection
    # if detection fails entirely we don't want to write garbage to S3
    try:
        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, NUMERIC_COLS, baseline, method="both")
        # logging - log that detection ran successfully
        logger.info(f"detection completed for {key}")
    except Exception as e:
        print(f"[ERROR] detection failed for {key}: {e}")
        # logging - log detection failures
        logger.error(f"detection failed for {key}: {e}")
        return None

    # 5. Write scored file to processed/ prefix
    # if this write fails we log it and stop — no point writing a summary for a file that didn't save
    try:
        output_key = key.replace("raw/", "processed/")
        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        print(f"[INFO] scored file written to s3://{bucket}/{output_key}")
        # logging - log successful scored file write
        logger.info(f"scored file written to s3://{bucket}/{output_key}")
    except ClientError as e:
        print(f"[ERROR] failed to write scored CSV to S3: {e}")
        # logging - log S3 write failures for scored output
        logger.error(f"failed to write scored CSV to S3: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] unexpected error writing scored CSV: {e}")
        # logging - catch all for scored file write failures
        logger.error(f"unexpected error writing scored CSV: {e}")
        return None

    # 6. Save updated baseline back to S3
    # non-fatal if this fails — we already scored the file, just log it
    try:
        baseline_mgr.save(baseline)
    except Exception as e:
        print(f"[ERROR] failed to save baseline after processing {key}: {e}")
        logger.error(f"failed to save baseline after processing {key}: {e}")

    # logging - sync the log file to S3 every time we save the baseline
    sync_log_to_s3(bucket)

    # 7. Build and return a processing summary
    try:
        anomaly_count = int(scored_df["anomaly"].sum()) if "anomaly" in scored_df else 0
        summary = {
            "source_key": key,
            "output_key": output_key,
            "processed_at": datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
            "baseline_observation_counts": {
                col: baseline.get(col, {}).get("count", 0) for col in NUMERIC_COLS
            }
        }

        # Write summary JSON alongside the processed file
        summary_key = output_key.replace(".csv", "_summary.json")
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )

        print(f"[INFO] done — {anomaly_count}/{len(df)} anomalies flagged")
        # logging - log the final result for each batch
        logger.info(f"done — {anomaly_count}/{len(df)} anomalies flagged for {key} (rate: {summary['anomaly_rate']})")
        return summary

    except Exception as e:
        print(f"[ERROR] failed to write summary JSON for {key}: {e}")
        # logging - log summary write failures
        logger.error(f"failed to write summary JSON for {key}: {e}")
        return None
    
    