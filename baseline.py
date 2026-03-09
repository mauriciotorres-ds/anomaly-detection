#!/usr/bin/env python3
import json
import math
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Optional

s3 = boto3.client("s3")


class BaselineManager:
    """
    Maintains a per-channel running baseline using Welford's online algorithm,
    which computes mean and variance incrementally without storing all past data.
    """

    def __init__(self, bucket: str, baseline_key: str = "state/baseline.json"):
        self.bucket = bucket
        self.baseline_key = baseline_key

    def load(self) -> dict:
        # using ClientError instead of s3.exceptions.NoSuchKey — more reliable across boto3 versions
        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.baseline_key)
            return json.loads(response["Body"].read())
        except ClientError as e:
            # first run — no baseline exists yet, just start fresh
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                print(f"[INFO] no existing baseline found, starting fresh")
                return {}
            # something else went wrong with S3
            print(f"[ERROR] failed to load baseline from S3: {e}")
            return {}
        except json.JSONDecodeError as e:
            # file exists but is corrupted or empty
            print(f"[ERROR] baseline file is invalid JSON: {e}")
            return {}
        except Exception as e:
            print(f"[ERROR] unexpected error loading baseline: {e}")
            return {}

    def save(self, baseline: dict):
        # wrapping the S3 write so a failed save doesn't crash the whole pipeline
        try:
            baseline["last_updated"] = datetime.utcnow().isoformat()
            s3.put_object(
                Bucket=self.bucket,
                Key=self.baseline_key,
                Body=json.dumps(baseline, indent=2),
                ContentType="application/json"
            )
            print(f"[INFO] baseline saved to s3://{self.bucket}/{self.baseline_key}")
        except ClientError as e:
            print(f"[ERROR] failed to save baseline to S3: {e}")
        except Exception as e:
            print(f"[ERROR] unexpected error saving baseline: {e}")

    def update(self, baseline: dict, channel: str, new_values: list[float]) -> dict:
        """
        Welford's online algorithm for numerically stable mean and variance.
        Each channel tracks: count, mean, M2 (sum of squared deviations).
        Variance = M2 / count, std = sqrt(variance).
        """
        try:
            if channel not in baseline:
                baseline[channel] = {"count": 0, "mean": 0.0, "M2": 0.0}

            state = baseline[channel]

            for value in new_values:
                # skipping bad values instead of letting one NaN or string blow up the whole update
                try:
                    state["count"] += 1
                    delta = value - state["mean"]
                    state["mean"] += delta / state["count"]
                    delta2 = value - state["mean"]
                    state["M2"] += delta * delta2
                except (TypeError, ValueError) as e:
                    print(f"[ERROR] skipping invalid value '{value}' for channel {channel}: {e}")
                    state["count"] -= 1  # undo the count increment for the bad value
                    continue

            # Only compute std once we have enough observations
            if state["count"] >= 2:
                variance = state["M2"] / state["count"]
                # guarding against tiny floating point negatives before sqrt
                state["std"] = math.sqrt(max(variance, 0))
            else:
                state["std"] = 0.0

            baseline[channel] = state
            print(f"[INFO] baseline updated — channel: {channel}, count: {state['count']}, mean: {round(state['mean'], 4)}")

        except Exception as e:
            print(f"[ERROR] unexpected error updating baseline for channel {channel}: {e}")

        return baseline

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        return baseline.get(channel)
