# app.py
import io
import json
import os
import boto3
import pandas as pd
import requests
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, Request
from baseline import BaselineManager
from processor import process_file

app = FastAPI(title="Anomaly Detection Pipeline")

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]

# ── SNS subscription confirmation + message handler ──────────────────────────

@app.post("/notify")
async def handle_sns(request: Request, background_tasks: BackgroundTasks):

    # catching malformed or non-JSON bodies so the server doesn't crash
    try:
        body = await request.json()
    except Exception as e:
        print(f"[ERROR] couldn't parse request body: {e}")
        return {"status": "error", "message": "invalid JSON body"}

    msg_type = request.headers.get("x-amz-sns-message-type")

    # SNS sends a SubscriptionConfirmation before it will deliver any messages.
    # Visiting the SubscribeURL confirms the subscription.
    if msg_type == "SubscriptionConfirmation":
        # wrapping the confirmation request in case the URL is unreachable
        try:
            confirm_url = body["SubscribeURL"]
            requests.get(confirm_url, timeout=10)
            print(f"[INFO] SNS subscription confirmed: {confirm_url}")
            return {"status": "confirmed"}
        except Exception as e:
            print(f"[ERROR] failed to confirm SNS subscription: {e}")
            return {"status": "error", "message": str(e)}

    if msg_type == "Notification":
        # wrapping notification parsing — bad S3 event format shouldn't take down the endpoint
        try:
            s3_event = json.loads(body["Message"])
            for record in s3_event.get("Records", []):
                key = record["s3"]["object"]["key"]
                if key.startswith("raw/") and key.endswith(".csv"):
                    print(f"[INFO] new file received, queuing for processing: {key}")
                    background_tasks.add_task(process_file, BUCKET_NAME, key)
        except Exception as e:
            print(f"[ERROR] failed to handle SNS notification: {e}")
            return {"status": "error", "message": str(e)}

    return {"status": "ok"}


# ── Query endpoints ───────────────────────────────────────────────────────────

@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = 50):
    """Return rows flagged as anomalies across the 10 most recent processed files."""

    # wrapping the whole thing so a bad S3 response doesn't break the endpoint
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        keys = sorted(
            [
                obj["Key"]
                for page in pages
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ],
            reverse=True,
        )[:10]

        all_anomalies = []
        for key in keys:
            # skipping individual files that fail so the rest still load
            try:
                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                df = pd.read_csv(io.BytesIO(response["Body"].read()))
                if "anomaly" in df.columns:
                    flagged = df[df["anomaly"] == True].copy()
                    flagged["source_file"] = key
                    all_anomalies.append(flagged)
            except Exception as e:
                print(f"[ERROR] couldn't read processed file {key}: {e}")
                continue

        if not all_anomalies:
            return {"count": 0, "anomalies": []}

        combined = pd.concat(all_anomalies).head(limit)
        return {"count": len(combined), "anomalies": combined.to_dict(orient="records")}

    except Exception as e:
        print(f"[ERROR] failed to retrieve recent anomalies: {e}")
        return {"error": str(e)}


@app.get("/anomalies/summary")
def get_anomaly_summary():
    """Aggregate anomaly rates across all processed files using their summary JSONs."""

    # wrapping the whole endpoint so S3 listing failures return a clean error
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        summaries = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_summary.json"):
                    # skipping individual summary files that are unreadable
                    try:
                        response = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                        summaries.append(json.loads(response["Body"].read()))
                    except Exception as e:
                        print(f"[ERROR] couldn't read summary file {obj['Key']}: {e}")
                        continue

        if not summaries:
            return {"message": "No processed files yet."}

        total_rows = sum(s["total_rows"] for s in summaries)
        total_anomalies = sum(s["anomaly_count"] for s in summaries)

        return {
            "files_processed": len(summaries),
            "total_rows_scored": total_rows,
            "total_anomalies": total_anomalies,
            "overall_anomaly_rate": round(total_anomalies / total_rows, 4) if total_rows > 0 else 0,
            "most_recent": sorted(summaries, key=lambda x: x["processed_at"], reverse=True)[:5],
        }

    except Exception as e:
        print(f"[ERROR] failed to retrieve anomaly summary: {e}")
        return {"error": str(e)}


@app.get("/baseline/current")
def get_current_baseline():
    """Show the current per-channel statistics the detector is working from."""

    # wrapping baseline load in case S3 is unavailable or file is missing
    try:
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()

        channels = {}
        for channel, stats in baseline.items():
            if channel == "last_updated":
                continue
            # skipping individual channels with malformed stats
            try:
                channels[channel] = {
                    "observations": stats["count"],
                    "mean": round(stats["mean"], 4),
                    "std": round(stats.get("std", 0.0), 4),
                    "baseline_mature": stats["count"] >= 30,
                }
            except Exception as e:
                print(f"[ERROR] couldn't parse stats for channel {channel}: {e}")
                continue

        return {
            "last_updated": baseline.get("last_updated"),
            "channels": channels,
        }

    except Exception as e:
        print(f"[ERROR] failed to retrieve baseline: {e}")
        return {"error": str(e)}


@app.get("/health")
def health():
    return {"status": "ok", "bucket": BUCKET_NAME, "timestamp": datetime.utcnow().isoformat()}