#!/usr/bin/env python3
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from typing import Optional


class AnomalyDetector:

    def __init__(self, z_threshold: float = 3.0, contamination: float = 0.05):
        self.z_threshold = z_threshold
        self.contamination = contamination  # expected fraction of anomalies

    def zscore_flag(
        self,
        values: pd.Series,
        mean: float,
        std: float
    ) -> pd.Series:
        """
        Flag values more than z_threshold standard deviations from the
        established baseline mean. Returns a Series of z-scores.
        """
        # wrapping this so a bad series or weird mean/std doesn't kill the whole run
        try:
            if std == 0:
                return pd.Series([0.0] * len(values))
            return (values - mean).abs() / std
        except Exception as e:
            print(f"[ERROR] failed to compute z-scores: {e}")
            # returning zeros so the column still exists and downstream code doesn't break
            return pd.Series([0.0] * len(values))

    def isolation_forest_flag(self, df: pd.DataFrame, numeric_cols: list[str]) -> np.ndarray:
        """
        Multivariate anomaly detection across all numeric channels simultaneously.
        IsolationForest returns -1 for anomalies, 1 for normal points.
        Scores closer to -1 indicate stronger anomalies.
        """
        try:
            # catching empty dataframes before sklearn throws a confusing error
            if df.empty:
                raise ValueError("dataframe is empty, can't run IsolationForest")

            # catching missing columns early so we know exactly what went wrong
            missing = [col for col in numeric_cols if col not in df.columns]
            if missing:
                raise ValueError(f"missing expected columns: {missing}")

            model = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100
            )
            X = df[numeric_cols].fillna(df[numeric_cols].median())
            model.fit(X)

            labels = model.predict(X)           # -1 = anomaly, 1 = normal
            scores = model.decision_function(X)  # lower = more anomalous

            return labels, scores

        except Exception as e:
            print(f"[ERROR] IsolationForest failed: {e}")
            # returning neutral values so the pipeline can keep going
            n = len(df)
            return np.ones(n, dtype=int), np.zeros(n)

    def run(
        self,
        df: pd.DataFrame,
        numeric_cols: list[str],
        baseline: dict,
        method: str = "both"
    ) -> pd.DataFrame:

        try:
            # nothing to score if the dataframe is empty
            if df.empty:
                print("[ERROR] empty dataframe passed to detector, skipping")
                return df

            # only work with columns that actually exist in this file
            valid_cols = [col for col in numeric_cols if col in df.columns]
            if not valid_cols:
                print(f"[ERROR] none of the expected columns {numeric_cols} found in dataframe")
                return df

            result = df.copy()

            # --- Z-score per channel ---
            if method in ("zscore", "both"):
                for col in valid_cols:
                    # wrapping per-column so one bad channel doesn't skip the rest
                    try:
                        stats = baseline.get(col)
                        if stats and stats["count"] >= 30:  # need enough history to trust baseline
                            z_scores = self.zscore_flag(df[col], stats["mean"], stats["std"])
                            result[f"{col}_zscore"] = z_scores.round(4)
                            result[f"{col}_zscore_flag"] = z_scores > self.z_threshold
                        else:
                            # not enough baseline history yet — flag as unknown
                            result[f"{col}_zscore"] = None
                            result[f"{col}_zscore_flag"] = None
                    except Exception as e:
                        print(f"[ERROR] z-score failed for column {col}: {e}")
                        result[f"{col}_zscore"] = None
                        result[f"{col}_zscore_flag"] = None

            # --- IsolationForest across all channels ---
            if method in ("isolation", "both"):
                labels, scores = self.isolation_forest_flag(df, valid_cols)
                result["if_label"] = labels           # -1 or 1
                result["if_score"] = scores.round(4)  # continuous anomaly score
                result["if_flag"] = labels == -1

            # --- Consensus flag: anomalous by at least one method ---
            if method == "both":
                try:
                    zscore_flags = [
                        result[f"{col}_zscore_flag"]
                        for col in valid_cols
                        if f"{col}_zscore_flag" in result.columns
                        and result[f"{col}_zscore_flag"].notna().any()
                    ]
                    if zscore_flags:
                        any_zscore = pd.concat(zscore_flags, axis=1).any(axis=1)
                        result["anomaly"] = any_zscore | result["if_flag"]
                    else:
                        result["anomaly"] = result["if_flag"]
                except Exception as e:
                    print(f"[ERROR] failed to build consensus anomaly flag: {e}")
                    # falling back to just the isolation forest flag
                    result["anomaly"] = result.get("if_flag", False)

            return result

        except Exception as e:
            print(f"[ERROR] unexpected error in detector.run(): {e}")
            return df
