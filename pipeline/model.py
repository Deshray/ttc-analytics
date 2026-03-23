"""
pipeline/model.py
Weather-driven transit reliability degradation model.

Predicts whether a route will fall below the TTC's on-time
performance target (80%) in a given 2-hour window, given
route history + weather conditions.

Output: route_risk_scores.xlsx with ML predictions appended.
        tomorrow_forecast.xlsx with next 7 days of risk forecasts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import logging
from pathlib import Path
from typing import Tuple, Dict

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from sklearn.calibration import CalibratedClassifierCV
import joblib

logger = logging.getLogger(__name__)

DATA_DIR   = Path("data")
MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# Feature set
# ─────────────────────────────────────────────
FEATURES = [
    # Time
    "hour", "day_of_week", "month", "is_weekend", "is_rush_hour",
    # Route history (risk signal)
    "route_mean_delay", "route_p95_delay", "route_sig_pct",
    "route_weather_sensitivity", "route_rush_sensitivity",
    # Weather (exogenous degradation drivers)
    "temp_c", "precip_mm", "snow_cm", "wind_kph",
    "weather_severity", "is_precipitation", "is_snow",
    "is_extreme_cold", "is_high_wind",
    # Encoded categoricals
    "route_enc", "time_period_enc",
]


# ─────────────────────────────────────────────
# Build training data from joined delay+weather
# ─────────────────────────────────────────────
def build_training_data(
    delays: pd.DataFrame,
    route_risk: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Target: route falls below 80% on-time in a 2-hour window.
    We aggregate delays to (route, date, 2hr_bucket) windows
    and label each window as degraded if on_time_pct < 80%.
    """
    df = delays.copy()

    # 2-hour buckets: 0-1, 2-3, 4-5, ...
    df["hour_bucket"] = (df["hour"] // 2) * 2

    # Aggregate to (route, date, hour_bucket)
    agg = df.groupby(["route", "date", "hour_bucket"]).agg(
        incidents     = ("delay_min", "count"),
        avg_delay     = ("delay_min", "mean"),
        on_time_pct   = ("is_on_time", "mean"),
        is_weekend    = ("is_weekend", "first"),
        day_of_week   = ("day_of_week", "first"),
        month         = ("month", "first"),
        is_rush_hour  = ("is_rush_hour", "max"),
        temp_c        = ("temp_c", "mean"),
        precip_mm     = ("precip_mm", "sum"),
        snow_cm       = ("snow_cm", "sum"),
        wind_kph      = ("wind_kph", "mean"),
        weather_severity    = ("weather_severity", "max"),
        is_precipitation    = ("is_precipitation", "max"),
        is_snow             = ("is_snow", "max"),
        is_extreme_cold     = ("is_extreme_cold", "max"),
        is_high_wind        = ("is_high_wind", "max"),
        time_period         = ("time_period", "first"),
    ).reset_index()

    agg["hour"] = agg["hour_bucket"]

    # Only keep windows with at least 3 incidents — avoids noise from
    # single-incident windows skewing the on-time calculation
    agg = agg[agg["incidents"] >= 3].copy()

    # Target: degraded if avg delay in this window exceeds the 60th
    # percentile for that route — route-relative threshold rather than
    # fixed 80% which doesn't account for route baseline differences
    route_median_delay = (
        df.groupby("route")["delay_min"]
          .quantile(0.60)
          .reset_index()
          .rename(columns={"delay_min": "route_p60_delay"})
    )
    agg = agg.merge(route_median_delay, on="route", how="left")
    agg["route_p60_delay"] = agg["route_p60_delay"].fillna(
        agg["avg_delay"].median()
    )
    # Degraded = this window's avg delay exceeds route's 60th percentile
    # AND on-time rate is below 70%
    agg["degraded"] = (
        (agg["avg_delay"] > agg["route_p60_delay"]) &
        (agg["on_time_pct"] < 0.70)
    ).astype(int)

    # Join route risk statistics
    route_feats = route_risk[[
        "route", "mean_delay_min", "p95_delay_min",
        "significant_delay_pct",
        "weather_sensitivity_precip", "rush_hour_sensitivity",
    ]].rename(columns={
        "mean_delay_min":           "route_mean_delay",
        "p95_delay_min":            "route_p95_delay",
        "significant_delay_pct":    "route_sig_pct",
        "weather_sensitivity_precip":"route_weather_sensitivity",
        "rush_hour_sensitivity":    "route_rush_sensitivity",
    })
    agg = agg.merge(route_feats, on="route", how="left")

    # Fill missing route stats
    for col in ["route_mean_delay","route_p95_delay","route_sig_pct",
                "route_weather_sensitivity","route_rush_sensitivity"]:
        agg[col] = agg[col].fillna(agg[col].median()
                                   if agg[col].notna().any() else 0)

    # Label encode
    le_route = LabelEncoder()
    agg["route_enc"] = le_route.fit_transform(agg["route"].astype(str))

    le_period = LabelEncoder()
    agg["time_period_enc"] = le_period.fit_transform(
        agg["time_period"].fillna("Unknown").astype(str)
    )

    # Save encoders
    joblib.dump({"route": le_route, "time_period": le_period},
                MODELS_DIR / "encoders.pkl")

    X = agg[FEATURES].fillna(0)
    y = agg["degraded"]

    return X, y, agg


# ─────────────────────────────────────────────
# Train
# ─────────────────────────────────────────────
def train_model(X: pd.DataFrame, y: pd.Series) -> Dict:
    logger.info(f"Training on {len(X):,} 2-hour window records | "
                f"base degradation rate: {y.mean():.1%}")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # GBT — primary
    gbt_base = GradientBoostingClassifier(
        n_estimators=200, learning_rate=0.06,
        max_depth=4, subsample=0.8,
        min_samples_leaf=20, random_state=42,
    )
    gbt = CalibratedClassifierCV(gbt_base, cv=3, method="isotonic")
    gbt.fit(X, y)

    # LR — interpretable
    lr = CalibratedClassifierCV(
        LogisticRegression(max_iter=1000, C=0.5, random_state=42),
        cv=3, method="sigmoid"
    )
    lr.fit(X_scaled, y)

    # OOF metrics
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    oof = np.zeros(len(y))
    for tr, val in cv.split(X, y):
        m = GradientBoostingClassifier(
            n_estimators=200, learning_rate=0.06,
            max_depth=4, subsample=0.8,
            min_samples_leaf=20, random_state=42,
        )
        m.fit(X.iloc[tr], y.iloc[tr])
        oof[val] = m.predict_proba(X.iloc[val])[:, 1]

    gbt_proba = gbt.predict_proba(X)[:, 1]
    lr_proba  = lr.predict_proba(X_scaled)[:, 1]

    # Feature importance from raw GBT
    raw = GradientBoostingClassifier(
        n_estimators=200, learning_rate=0.06,
        max_depth=4, subsample=0.8,
        min_samples_leaf=20, random_state=42,
    )
    raw.fit(X, y)
    fi = pd.DataFrame({
        "feature":    FEATURES,
        "importance": raw.feature_importances_,
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    metrics = {
        "gbt_auc":     float(roc_auc_score(y, gbt_proba)),
        "gbt_oof_auc": float(roc_auc_score(y, oof)),
        "gbt_pr_auc":  float(average_precision_score(y, gbt_proba)),
        "gbt_brier":   float(brier_score_loss(y, gbt_proba)),
        "lr_auc":      float(roc_auc_score(y, lr_proba)),
        "base_rate":   float(y.mean()),
        "n_windows":   int(len(y)),
        "feature_importances": fi,
    }

    joblib.dump(gbt,    MODELS_DIR / "gbt.pkl")
    joblib.dump(lr,     MODELS_DIR / "lr.pkl")
    joblib.dump(scaler, MODELS_DIR / "scaler.pkl")

    logger.info(f"  GBT AUC: {metrics['gbt_auc']:.4f} | "
                f"OOF AUC: {metrics['gbt_oof_auc']:.4f}")

    return metrics, gbt, scaler


# ─────────────────────────────────────────────
# Generate 7-day forecast table
# ─────────────────────────────────────────────
def generate_forecast(
    route_risk: pd.DataFrame,
    forecast_weather: pd.DataFrame,
    gbt,
    scaler: StandardScaler,
) -> pd.DataFrame:
    """
    For each (route, day, 2hr_bucket) in the next 7 days,
    predict degradation probability given forecast weather.
    Returns a table suitable for Power BI's forecast page.
    """
    encoders = joblib.load(MODELS_DIR / "encoders.pkl")
    le_route  = encoders["route"]
    le_period = encoders["time_period"]

    # Expand: every route × every forecast hour
    routes = route_risk["route"].astype(str).tolist()
    rows   = []

    for _, wrow in forecast_weather.iterrows():
        dt     = pd.to_datetime(wrow["datetime"])
        hb     = (int(wrow["hour"]) // 2) * 2
        period = _hour_to_period(wrow["hour"])

        for route in routes:
            r_stats = route_risk[route_risk["route"].astype(str) == route]
            if r_stats.empty:
                continue
            r = r_stats.iloc[0]

            # Encode route — handle unseen routes
            known = set(le_route.classes_)
            r_enc = int(le_route.transform([route])[0]) if route in known else -1
            p_enc_val = period if period in set(le_period.classes_) else "Unknown"
            p_enc = int(le_period.transform([p_enc_val])[0]) \
                    if p_enc_val in set(le_period.classes_) else 0

            row = {
                "route":          route,
                "datetime":       dt,
                "date":           dt.date(),
                "day_name":       dt.strftime("%A"),
                "hour":           hb,
                "time_period":    period,
                "hour_bucket":    hb,
                "day_of_week":    dt.weekday(),
                "month":          dt.month,
                "is_weekend":     int(dt.weekday() >= 5),
                "is_rush_hour":   int(7 <= hb <= 9 or 16 <= hb <= 19),
                "route_mean_delay":        r.get("mean_delay_min", 0),
                "route_p95_delay":         r.get("p95_delay_min", 0),
                "route_sig_pct":           r.get("significant_delay_pct", 0),
                "route_weather_sensitivity": r.get("weather_sensitivity_precip", 0),
                "route_rush_sensitivity":  r.get("rush_hour_sensitivity", 0),
                "temp_c":          wrow.get("temp_c", 10),
                "precip_mm":       wrow.get("precip_mm", 0),
                "snow_cm":         wrow.get("snow_cm", 0),
                "wind_kph":        wrow.get("wind_kph", 15),
                "weather_severity":wrow.get("weather_severity", 0),
                "is_precipitation":wrow.get("is_precipitation", 0),
                "is_snow":         wrow.get("is_snow", 0),
                "is_extreme_cold": wrow.get("is_extreme_cold", 0),
                "is_high_wind":    wrow.get("is_high_wind", 0),
                "weather_condition": wrow.get("weather_condition", "Unknown"),
                "route_enc":       r_enc,
                "time_period_enc": p_enc,
                "risk_score":      float(r.get("risk_score", 0)),
                "risk_tier":       str(r.get("risk_tier", "Low")),
            }
            rows.append(row)

    forecast_df = pd.DataFrame(rows)
    if forecast_df.empty:
        return forecast_df

    X_fc = forecast_df[FEATURES].fillna(0)
    forecast_df["degradation_prob"] = gbt.predict_proba(X_fc)[:, 1]

    # Composite alert score: weight degradation prob by route risk
    forecast_df["alert_score"] = (
        0.6 * forecast_df["degradation_prob"] +
        0.4 * forecast_df["risk_score"]
    ).round(4)

    forecast_df["alert_level"] = pd.cut(
        forecast_df["alert_score"],
        bins=[-0.001, 0.25, 0.45, 0.65, 1.0],
        labels=["Normal", "Watch", "Warning", "Alert"],
    )

    return forecast_df.sort_values(
        ["date", "alert_score"], ascending=[True, False]
    ).reset_index(drop=True)


def _hour_to_period(h) -> str:
    try:
        h = int(h)
    except (ValueError, TypeError):
        return "Unknown"
    if 5  <= h < 7:  return "Early Morning"
    if 7  <= h < 10: return "AM Rush"
    if 10 <= h < 16: return "Midday"
    if 16 <= h < 20: return "PM Rush"
    if 20 <= h < 24: return "Evening"
    return "Night"