#!/usr/bin/env python3
import json
import io
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime

from baseline import BaselineManager
from detector import AnomalyDetector

s3 = boto3.client("s3")

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]  # students configure this


def process_file(bucket: str, key: str):
    print(f"[INFO] processing: s3://{bucket}/{key}")

    # 1. Download raw file
    # if we can't get the file there's nothing to do — bail out early
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        print(f"[INFO] loaded {len(df)} rows, columns: {list(df.columns)}")
    except ClientError as e:
        print(f"[ERROR] couldn't download s3://{bucket}/{key}: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] failed to read CSV from s3://{bucket}/{key}: {e}")
        return None

    # no point continuing if the file came back empty
    if df.empty:
        print(f"[ERROR] file s3://{bucket}/{key} is empty, skipping")
        return None

    # 2. Load current baseline
    # if baseline manager fails to init we can't score anything reliably
    try:
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()
    except Exception as e:
        print(f"[ERROR] failed to initialize baseline manager: {e}")
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
                continue

    # 4. Run detection
    # if detection fails entirely we don't want to write garbage to S3
    try:
        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, NUMERIC_COLS, baseline, method="both")
    except Exception as e:
        print(f"[ERROR] detection failed for {key}: {e}")
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
    except ClientError as e:
        print(f"[ERROR] failed to write scored CSV to S3: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] unexpected error writing scored CSV: {e}")
        return None

    # 6. Save updated baseline back to S3
    # non-fatal if this fails — we already scored the file, just log it
    try:
        baseline_mgr.save(baseline)
    except Exception as e:
        print(f"[ERROR] failed to save baseline after processing {key}: {e}")

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
        return summary

    except Exception as e:
        print(f"[ERROR] failed to write summary JSON for {key}: {e}")
        return None