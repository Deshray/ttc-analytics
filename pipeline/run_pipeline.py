"""
pipeline/run_pipeline.py
Master pipeline — run this once to produce all Power BI data files.

What it does:
  1. Fetches TTC bus delay data from Toronto Open Data
  2. Fetches historical weather from Open-Meteo API
  3. Joins delay + weather records
  4. Runs all SQL transformations via DuckDB
  5. Trains the weather-driven reliability degradation model
  6. Generates the 7-day forecast table
  7. Exports all tables as .xlsx files for Power BI

Output files (in data/output/):
  route_daily.xlsx         — route × date reliability + weather
  route_hourly.xlsx        — route × hour heatmap data
  weather_impact.xlsx      — delay lift by weather condition
  network_summary.xlsx     — daily network KPIs
  route_risk_scores.xlsx   — composite risk scores + ML predictions
  tomorrow_forecast.xlsx   — 7-day risk forecast

Usage:
  python pipeline/run_pipeline.py
  python pipeline/run_pipeline.py --year 2023
  python pipeline/run_pipeline.py --year 2024 --no-cache
"""

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np
import joblib

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.fetch_data import (
    fetch_ttc_bus_delays, clean_ttc_delays,
    fetch_weather, fetch_weather_forecast,
    join_delays_weather,
)
from pipeline.model import (
    build_training_data, train_model, generate_forecast,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR   = Path("data")
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run(year: int = 2024, use_cache: bool = True):
    logger.info("=" * 55)
    logger.info("TTC TRANSIT RELIABILITY ANALYTICS — PIPELINE START")
    logger.info("=" * 55)

    # ── Step 1: Fetch TTC data ────────────────────────────
    logger.info("\n[1/6] Fetching TTC bus delay data…")
    raw = fetch_ttc_bus_delays(years=[year])
    delays_clean = clean_ttc_delays(raw)
    logger.info(f"      {len(delays_clean):,} clean delay records | "
                f"{delays_clean['route'].nunique()} routes")

    # ── Step 2: Fetch weather ─────────────────────────────
    logger.info("\n[2/6] Fetching historical weather data…")
    start_date = f"{year}-01-01"
    end_date   = f"{year}-12-31"
    weather    = fetch_weather(start_date, end_date)
    logger.info(f"      {len(weather):,} hourly weather records")

    # ── Step 3: Join ──────────────────────────────────────
    logger.info("\n[3/6] Joining delays + weather…")
    delays = join_delays_weather(delays_clean, weather)
    logger.info(f"      {len(delays):,} joined records | "
                f"weather match rate: "
                f"{(delays['temp_c'] != 10).mean():.1%}")

    # ── Step 4: SQL transforms via DuckDB ─────────────────
    logger.info("\n[4/6] Running SQL transformations (DuckDB)…")
    con = duckdb.connect()
    con.register("delays", delays)

    # Execute all SQL from transform.sql
    sql_path = Path("sql/transform.sql")
    if not sql_path.exists():
        # Inline fallback if sql file not found
        sql_path = Path(__file__).parent.parent / "sql" / "transform.sql"

    sql = sql_path.read_text()

    # Split on CREATE OR REPLACE TABLE and execute each block
    statements = [s.strip() for s in sql.split("CREATE OR REPLACE TABLE")
                  if s.strip()]
    for stmt in statements:
        if not stmt:
            continue
        # Skip comment-only blocks
        content = "\n".join(
            l for l in stmt.split("\n")
            if not l.strip().startswith("--") and l.strip()
        )
        if not content.strip():
            continue
        try:
            con.execute("CREATE OR REPLACE TABLE " + stmt)
        except Exception as e:
            logger.warning(f"  SQL warning: {e}")

    # Export SQL output tables
    sql_tables = {
        "route_daily":    "route_daily.xlsx",
        "route_hourly":   "route_hourly.xlsx",
        "weather_impact": "weather_impact.xlsx",
        "network_summary":"network_summary.xlsx",
        "route_risk_scores": "route_risk_scores_base.xlsx",
    }
    sql_dfs = {}
    for table, fname in sql_tables.items():
        try:
            df = con.execute(f"SELECT * FROM {table}").df()
            sql_dfs[table] = df
            df.to_excel(OUTPUT_DIR / fname, index=False)
            logger.info(f"  ✓ {table}: {len(df):,} rows → {fname}")
        except Exception as e:
            logger.warning(f"  ✗ {table}: {e}")
            sql_dfs[table] = pd.DataFrame()

    con.close()

    # ── Step 5: Train ML model ────────────────────────────
    logger.info("\n[5/6] Training weather-driven degradation model…")
    route_risk = sql_dfs.get("route_risk_scores", pd.DataFrame())

    if route_risk.empty:
        logger.warning("  Route risk scores unavailable — computing from delays")
        route_risk = _fallback_route_risk(delays)

    X, y, agg = build_training_data(delays, route_risk)
    metrics, gbt, scaler = train_model(X, y)

    logger.info(f"  GBT AUC:     {metrics['gbt_auc']:.4f}")
    logger.info(f"  OOF AUC:     {metrics['gbt_oof_auc']:.4f}")
    logger.info(f"  Base rate:   {metrics['base_rate']:.1%} windows degraded")

    # Save model metrics
    pd.DataFrame([{
        "metric": k, "value": v
    } for k, v in metrics.items()
    if not isinstance(v, pd.DataFrame)]).to_excel(
        OUTPUT_DIR / "model_metrics.xlsx", index=False
    )

    # Save feature importance
    if isinstance(metrics.get("feature_importances"), pd.DataFrame):
        metrics["feature_importances"].to_excel(
            OUTPUT_DIR / "feature_importance.xlsx", index=False
        )
        logger.info("  ✓ feature_importance.xlsx")

    # Append ML risk scores to route_risk_scores
    if not route_risk.empty:
        # Add model-predicted degradation probability for each route
        # using median conditions for each route
        route_risk_ml = _add_ml_risk(route_risk, gbt, scaler, delays)
        route_risk_ml.to_excel(
            OUTPUT_DIR / "route_risk_scores.xlsx", index=False
        )
        logger.info(f"  ✓ route_risk_scores.xlsx: {len(route_risk_ml):,} routes")

    # ── Step 6: Generate 7-day forecast ───────────────────
    logger.info("\n[6/6] Generating 7-day risk forecast…")
    try:
        forecast_weather = fetch_weather_forecast()
        forecast_df = generate_forecast(
            route_risk_ml if not route_risk.empty else route_risk,
            forecast_weather,
            gbt, scaler,
        )

        if not forecast_df.empty:
            # Summary: top risks per day
            forecast_summary = (
                forecast_df.groupby(["date", "day_name", "route"])
                .agg(
                    max_degradation_prob = ("degradation_prob", "max"),
                    max_alert_score      = ("alert_score", "max"),
                    alert_level          = ("alert_level", lambda x:
                        x.value_counts().index[0] if len(x) > 0 else "Normal"),
                    weather_condition    = ("weather_condition", "first"),
                    risk_tier            = ("risk_tier", "first"),
                )
                .reset_index()
                .sort_values(["date", "max_alert_score"], ascending=[True, False])
            )
            forecast_summary.to_excel(
                OUTPUT_DIR / "tomorrow_forecast.xlsx", index=False
            )
            logger.info(f"  ✓ tomorrow_forecast.xlsx: "
                        f"{len(forecast_summary):,} route-day combinations")
    except Exception as e:
        logger.warning(f"  Forecast generation failed: {e}")
        logger.info("  (Forecast requires internet connection)")

    # ── Done ──────────────────────────────────────────────
    logger.info("\n" + "=" * 55)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Output files in: {OUTPUT_DIR.resolve()}")
    logger.info("=" * 55)
    logger.info("\nFiles ready for Power BI:")
    for f in sorted(OUTPUT_DIR.glob("*.xlsx")):
        size_kb = f.stat().st_size / 1024
        logger.info(f"  {f.name:<40} {size_kb:>6.1f} KB")

    return {
        "delays": delays,
        "route_risk": route_risk_ml if not route_risk.empty else route_risk,
        "metrics": metrics,
    }


def _fallback_route_risk(delays: pd.DataFrame) -> pd.DataFrame:
    """Simple route risk computation if SQL transform fails."""
    grp = delays.groupby("route").agg(
        total_incidents         = ("delay_min", "count"),
        mean_delay_min          = ("delay_min", "mean"),
        median_delay_min        = ("delay_min", "median"),
        p95_delay_min           = ("delay_min", lambda x: np.percentile(x, 95)),
        on_time_pct             = ("is_on_time", lambda x: x.mean() * 100),
        significant_delay_pct   = ("is_significant", lambda x: x.mean() * 100),
        severe_delay_pct        = ("is_severe", lambda x: x.mean() * 100),
        total_delay_hrs         = ("delay_min", lambda x: x.sum() / 60),
    ).reset_index()
    grp["weather_sensitivity_precip"] = 0.0
    grp["rush_hour_sensitivity"]      = 0.0
    for col in ["significant_delay_pct","mean_delay_min","p95_delay_min","total_incidents"]:
        rng = grp[col].max() - grp[col].min()
        grp[f"{col}_norm"] = (grp[col] - grp[col].min()) / rng if rng > 0 else 0
    grp["risk_score"] = (
        0.35 * grp["significant_delay_pct_norm"] +
        0.30 * grp["mean_delay_min_norm"] +
        0.20 * grp["p95_delay_min_norm"] +
        0.15 * grp["total_incidents_norm"]
    ).round(4)
    grp["risk_tier"] = pd.cut(
        grp["risk_score"],
        bins=[-0.001, 0.35, 0.55, 0.75, 1.0],
        labels=["Low","Medium","High","Critical"]
    )
    grp["risk_rank"] = grp["risk_score"].rank(ascending=False).astype(int)
    return grp.sort_values("risk_score", ascending=False).reset_index(drop=True)


def _add_ml_risk(
    route_risk: pd.DataFrame, gbt, scaler, delays: pd.DataFrame
) -> pd.DataFrame:
    """Append ML-predicted degradation probability to route risk table."""
    from pipeline.model import FEATURES, _hour_to_period
    from sklearn.preprocessing import LabelEncoder
    import joblib
    from pathlib import Path

    encoders = joblib.load(Path("models") / "encoders.pkl")
    le_route  = encoders["route"]
    le_period = encoders["time_period"]

    rows = []
    known_routes  = set(le_route.classes_)
    known_periods = set(le_period.classes_)

    for _, r in route_risk.iterrows():
        route = str(r["route"])
        r_enc = int(le_route.transform([route])[0]) if route in known_routes else -1

        # Worst case: PM rush, Wednesday, rain
        period     = "PM Rush"
        period_enc = int(le_period.transform([period])[0]) \
                     if period in known_periods else 0

        feat_row = {
            "hour": 17, "day_of_week": 2, "month": 11,
            "is_weekend": 0, "is_rush_hour": 1,
            "route_mean_delay":          r.get("mean_delay_min", 0),
            "route_p95_delay":           r.get("p95_delay_min", 0),
            "route_sig_pct":             r.get("significant_delay_pct", 0),
            "route_weather_sensitivity": r.get("weather_sensitivity_precip", 0),
            "route_rush_sensitivity":    r.get("rush_hour_sensitivity", 0),
            "temp_c": 2, "precip_mm": 3, "snow_cm": 0,
            "wind_kph": 25, "weather_severity": 2,
            "is_precipitation": 1, "is_snow": 0,
            "is_extreme_cold": 0, "is_high_wind": 0,
            "route_enc": r_enc, "time_period_enc": period_enc,
        }
        feat_df = pd.DataFrame([feat_row])[FEATURES].fillna(0)
        prob = float(gbt.predict_proba(feat_df)[0, 1])
        rows.append(prob)

    route_risk = route_risk.copy()
    route_risk["ml_degradation_prob_worst_case"] = rows
    route_risk["ml_degradation_prob_worst_case"] = \
        route_risk["ml_degradation_prob_worst_case"].round(4)

    return route_risk


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TTC Transit Reliability Analytics Pipeline"
    )
    parser.add_argument("--year",     type=int, default=2024)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    run(year=args.year, use_cache=not args.no_cache)