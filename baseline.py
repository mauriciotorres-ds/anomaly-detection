#!/usr/bin/env python3
import json
import math
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Optional
# logging - importing logging so baseline events get written to the same log file as app.py
import logging

s3 = boto3.client("s3")

# logging - using the same logger as app.py so everything goes to one file
logger = logging.getLogger(__name__)


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
            data = json.loads(response["Body"].read())
            # logging - log successful baseline load so we know state was restored
            logger.info(f"baseline loaded from s3://{self.bucket}/{self.baseline_key}")
            return data
        except ClientError as e:
            # first run — no baseline exists yet, just start fresh
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                print(f"[INFO] no existing baseline found, starting fresh")
                # logging - log first run so we know this is a clean start
                logger.info("no existing baseline found, starting fresh")
                return {}
            # something else went wrong with S3
            print(f"[ERROR] failed to load baseline from S3: {e}")
            # logging - log S3 errors on load
            logger.error(f"failed to load baseline from S3: {e}")
            return {}
        except json.JSONDecodeError as e:
            # file exists but is corrupted or empty
            print(f"[ERROR] baseline file is invalid JSON: {e}")
            # logging - log corruption so we know if the state file got mangled
            logger.error(f"baseline file is invalid JSON: {e}")
            return {}
        except Exception as e:
            print(f"[ERROR] unexpected error loading baseline: {e}")
            # logging - catch all for unexpected load failures
            logger.error(f"unexpected error loading baseline: {e}")
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
            # logging - log every baseline save so we have a record of each state sync to S3
            logger.info(f"baseline saved to s3://{self.bucket}/{self.baseline_key}")
        except ClientError as e:
            print(f"[ERROR] failed to save baseline to S3: {e}")
            # logging - log S3 write failures so we know if state is going out of sync
            logger.error(f"failed to save baseline to S3: {e}")
        except Exception as e:
            print(f"[ERROR] unexpected error saving baseline: {e}")
            # logging - catch all for unexpected save failures
            logger.error(f"unexpected error saving baseline: {e}")

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
                    # logging - log individual bad values so we can spot data quality issues
                    logger.error(f"skipping invalid value '{value}' for channel {channel}: {e}")
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
            # logging - log each baseline update so we can track how the stats evolve over time
            logger.info(f"baseline updated — channel: {channel}, count: {state['count']}, mean: {round(state['mean'], 4)}, std: {round(state['std'], 4)}")

        except Exception as e:
            print(f"[ERROR] unexpected error updating baseline for channel {channel}: {e}")
            # logging - catch all for unexpected update failures
            logger.error(f"unexpected error updating baseline for channel {channel}: {e}")

        return baseline

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        return baseline.get(channel)