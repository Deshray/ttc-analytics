"""
pipeline/fetch_data.py
Fetches TTC bus delay data from Toronto Open Data and
hourly weather data from Open-Meteo API (no API key required).

Run directly: python pipeline/fetch_data.py
Or via run_pipeline.py
"""

from __future__ import annotations

import io
import time
import logging
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
DATA_DIR  = Path("data")
RAW_DIR   = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# Toronto Open Data — TTC Bus Delays
# ─────────────────────────────────────────────
CKAN_BASE    = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
BUS_PKG_ID   = "e271cdae-8788-4980-96ce-6a5c95bc6618"

def get_package_resources(package_id: str) -> list[dict]:
    url  = f"{CKAN_BASE}/api/3/action/package_show"
    resp = requests.get(url, params={"id": package_id}, timeout=30)
    resp.raise_for_status()
    return resp.json()["result"]["resources"]


def fetch_ttc_bus_delays(years: list[int] = None) -> pd.DataFrame:
    """
    Download TTC bus delay Excel files from Toronto Open Data.
    Returns concatenated raw DataFrame.
    """
    if years is None:
        years = [2024]

    cache = RAW_DIR / f"ttc_bus_raw_{'_'.join(map(str, years))}.parquet"
    if cache.exists():
        logger.info(f"Loading cached TTC data from {cache}")
        return pd.read_parquet(cache)

    logger.info(f"Fetching TTC bus delay data for {years} from Toronto Open Data…")
    resources = get_package_resources(BUS_PKG_ID)

    frames = []
    for res in resources:
        name = res.get("name", "").lower()
        fmt  = res.get("format", "").upper()
        if not any(str(y) in name for y in years):
            continue
        if fmt not in ("XLSX", "CSV", "XLS"):
            continue

        logger.info(f"  Downloading: {res['name']}")
        try:
            r = requests.get(res["url"], timeout=90)
            r.raise_for_status()
            if "XLS" in fmt:
                df = pd.read_excel(io.BytesIO(r.content))
            else:
                df = pd.read_csv(io.BytesIO(r.content))
            frames.append(df)
            time.sleep(0.5)   # be polite to the API
        except Exception as e:
            logger.warning(f"  Failed to download {res['name']}: {e}")

    if not frames:
        raise RuntimeError(
            "No TTC data could be fetched. "
            "Check your internet connection or try again later."
        )

    raw = pd.concat(frames, ignore_index=True)
    logger.info(f"  Downloaded {len(raw):,} raw records")

    # Convert all object columns to string so pyarrow can serialize them
    for col in raw.select_dtypes(include="object").columns:
        raw[col] = raw[col].astype(str)

    raw.to_parquet(cache, index=False)
    return raw


# ─────────────────────────────────────────────
# Clean TTC delay records
# ─────────────────────────────────────────────

# TTC incident code → human-readable category
INCIDENT_CATEGORIES = {
    "Mechanical":     ["Mechanical", "Held By", "Vehicle Out of Service"],
    "Operator":       ["Operator", "Late Leaving Garage", "Sign Availability"],
    "Traffic":        ["Traffic", "Diversion", "Road Blocked", "Utilized Off Route"],
    "Passenger":      ["Emergency Services", "Investigation", "Security",
                       "Injured or Ill Operator", "Injured or Ill Customer"],
    "Infrastructure": ["Cleaning", "General Delay", "Vision"],
}

def _categorize(incident: str) -> str:
    if pd.isna(incident):
        return "Other"
    s = str(incident).upper()
    for cat, keywords in INCIDENT_CATEGORIES.items():
        if any(k.upper() in s for k in keywords):
            return cat
    return "Other"


def clean_ttc_delays(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Rename variants
    df = df.rename(columns={
        "report_date": "date", "min_delay": "delay_min",
        "min_gap": "gap_min", "route": "route",
        "time": "time", "incident": "incident",
        "direction": "direction", "location": "location",
        "vehicle": "vehicle_id",
    })

    # Date
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df = df.dropna(subset=["date"])

    # Time → hour
    if "time" in df.columns:
        df["hour"] = pd.to_datetime(
            df["time"].astype(str), format="%H:%M", errors="coerce"
        ).dt.hour
        df["hour"] = df["hour"].fillna(
            pd.to_numeric(df["time"].astype(str).str[:2], errors="coerce")
        ).fillna(0).astype(int)
    else:
        df["hour"] = 0

    # Delay numeric
    df["delay_min"] = pd.to_numeric(df.get("delay_min", 0), errors="coerce").fillna(0)
    df = df[df["delay_min"] >= 0].copy()

    # Derived time features
    df["day_of_week"]  = df["date"].dt.dayofweek          # 0=Mon
    df["day_name"]     = df["date"].dt.day_name()
    df["month"]        = df["date"].dt.month
    df["month_name"]   = df["date"].dt.strftime("%b")
    df["week"]         = df["date"].dt.isocalendar().week.astype(int)
    df["is_weekend"]   = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_rush_hour"] = df["hour"].apply(
        lambda h: 1 if (7 <= h <= 9 or 16 <= h <= 19) else 0
    )
    df["time_period"]  = df["hour"].apply(_hour_to_period)

    # Reliability threshold — on-time = delay < 5 min
    df["is_on_time"]         = (df["delay_min"] < 5).astype(int)
    df["is_significant"]     = (df["delay_min"] >= 5).astype(int)
    df["is_severe"]          = (df["delay_min"] >= 20).astype(int)

    # Incident category
    if "incident" in df.columns:
        df["incident_category"] = df["incident"].apply(_categorize)
    else:
        df["incident_category"] = "Other"

    # Route as string
    if "route" in df.columns:
        df["route"] = df["route"].astype(str).str.strip().str.upper()

    return df.reset_index(drop=True)


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


# ─────────────────────────────────────────────
# Open-Meteo — historical hourly weather
# Toronto: lat=43.7001, lon=-79.4163
# ─────────────────────────────────────────────
TORONTO_LAT = 43.7001
TORONTO_LON = -79.4163

WEATHER_VARS = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "windspeed_10m",
    "weathercode",
]

def fetch_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch hourly historical weather for Toronto from Open-Meteo.
    Free, no API key required.
    start_date / end_date: 'YYYY-MM-DD'
    """
    cache = RAW_DIR / f"weather_{start_date}_{end_date}.parquet"
    if cache.exists():
        logger.info(f"Loading cached weather from {cache}")
        return pd.read_parquet(cache)

    logger.info(f"Fetching weather data {start_date} → {end_date} from Open-Meteo…")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":       TORONTO_LAT,
        "longitude":      TORONTO_LON,
        "start_date":     start_date,
        "end_date":       end_date,
        "hourly":         ",".join(WEATHER_VARS),
        "timezone":       "America/Toronto",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()["hourly"]

    df = pd.DataFrame(data)
    df["datetime"] = pd.to_datetime(df["time"])
    df["date"]     = df["datetime"].dt.date
    df["hour"]     = df["datetime"].dt.hour
    df = df.drop(columns=["time"])

    # Rename for clarity
    df = df.rename(columns={
        "temperature_2m": "temp_c",
        "precipitation":  "precip_mm",
        "snowfall":       "snow_cm",
        "windspeed_10m":  "wind_kph",
        "weathercode":    "weather_code",
    })

    # Weather condition from WMO code
    df["weather_condition"] = df["weather_code"].apply(_wmo_to_condition)

    # Derived features
    df["is_precipitation"] = (df["precip_mm"] > 0.2).astype(int)
    df["is_heavy_precip"]  = (df["precip_mm"] > 5.0).astype(int)
    df["is_snow"]          = (df["snow_cm"] > 0.1).astype(int)
    df["is_extreme_cold"]  = (df["temp_c"] < -10).astype(int)
    df["is_high_wind"]     = (df["wind_kph"] > 40).astype(int)
    df["weather_severity"] = (
        df["is_heavy_precip"] * 3 +
        df["is_snow"] * 2 +
        df["is_precipitation"] * 1 +
        df["is_extreme_cold"] * 2 +
        df["is_high_wind"] * 1
    ).clip(0, 5)

    df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(cache, index=False)
    logger.info(f"  {len(df):,} hourly weather records fetched")
    return df


def fetch_weather_forecast() -> pd.DataFrame:
    """Fetch 7-day hourly weather forecast for Toronto."""
    logger.info("Fetching 7-day weather forecast from Open-Meteo…")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  TORONTO_LAT,
        "longitude": TORONTO_LON,
        "hourly":    ",".join(WEATHER_VARS),
        "timezone":  "America/Toronto",
        "forecast_days": 7,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()["hourly"]

    df = pd.DataFrame(data)
    df["datetime"] = pd.to_datetime(df["time"])
    df["date"]     = df["datetime"].dt.normalize()
    df["hour"]     = df["datetime"].dt.hour
    df = df.drop(columns=["time"])
    df = df.rename(columns={
        "temperature_2m": "temp_c",
        "precipitation":  "precip_mm",
        "snowfall":       "snow_cm",
        "windspeed_10m":  "wind_kph",
        "weathercode":    "weather_code",
    })
    df["weather_condition"] = df["weather_code"].apply(_wmo_to_condition)
    df["is_precipitation"]  = (df["precip_mm"] > 0.2).astype(int)
    df["is_heavy_precip"]   = (df["precip_mm"] > 5.0).astype(int)
    df["is_snow"]           = (df["snow_cm"] > 0.1).astype(int)
    df["is_extreme_cold"]   = (df["temp_c"] < -10).astype(int)
    df["is_high_wind"]      = (df["wind_kph"] > 40).astype(int)
    df["weather_severity"]  = (
        df["is_heavy_precip"] * 3 + df["is_snow"] * 2 +
        df["is_precipitation"] * 1 + df["is_extreme_cold"] * 2 +
        df["is_high_wind"] * 1
    ).clip(0, 5)
    return df


def _wmo_to_condition(code) -> str:
    """Map WMO weather codes to human-readable conditions."""
    try:
        code = int(code)
    except (ValueError, TypeError):
        return "Unknown"
    if code == 0:               return "Clear"
    if code in (1, 2, 3):       return "Cloudy"
    if code in (45, 48):        return "Fog"
    if code in (51, 53, 55):    return "Drizzle"
    if code in (61, 63, 65):    return "Rain"
    if code in (71, 73, 75, 77):return "Snow"
    if code in (80, 81, 82):    return "Rain Showers"
    if code in (85, 86):        return "Snow Showers"
    if code in (95, 96, 99):    return "Thunderstorm"
    return "Other"


# ─────────────────────────────────────────────
# Join TTC delays + weather
# ─────────────────────────────────────────────
def join_delays_weather(
    delays: pd.DataFrame,
    weather: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join delay records to hourly weather on (date, hour).
    """
    weather_join = weather[["date", "hour", "temp_c", "precip_mm",
                             "snow_cm", "wind_kph", "weather_condition",
                             "is_precipitation", "is_heavy_precip",
                             "is_snow", "is_extreme_cold", "is_high_wind",
                             "weather_severity"]].copy()

    delays["date_join"] = pd.to_datetime(delays["date"].dt.date)
    weather_join["date_join"] = pd.to_datetime(weather_join["date"].dt.date
                                               if hasattr(weather_join["date"].dt, "date")
                                               else weather_join["date"])

    merged = delays.merge(
        weather_join.rename(columns={"date_join": "date_join_w"}),
        left_on=["date_join", "hour"],
        right_on=["date_join_w", "hour"],
        how="left",
        suffixes=("", "_w"),
    )
    merged = merged.drop(columns=["date_join", "date_join_w"], errors="ignore")

    # Fill missing weather with neutral values
    merged["temp_c"]           = merged["temp_c"].fillna(10)
    merged["precip_mm"]        = merged["precip_mm"].fillna(0)
    merged["snow_cm"]          = merged["snow_cm"].fillna(0)
    merged["wind_kph"]         = merged["wind_kph"].fillna(15)
    merged["weather_condition"]= merged["weather_condition"].fillna("Unknown")
    merged["weather_severity"] = merged["weather_severity"].fillna(0)
    for col in ["is_precipitation","is_heavy_precip","is_snow",
                "is_extreme_cold","is_high_wind"]:
        merged[col] = merged[col].fillna(0).astype(int)

    return merged.reset_index(drop=True)