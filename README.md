# 🚌 TTC Transit Reliability Analytics

**Weather-driven service degradation prediction and operations dashboard for the Toronto Transit Commission.**

🔗 **[Live Power BI Dashboard](your-link-here)**

---

## What This Is

A data analytics + ML pipeline that answers a real operational question: **given tomorrow's weather forecast, which TTC bus routes are most likely to fall below their on-time performance target — and what should operations do about it?**

**Three layers:**

1. **Data pipeline (Python + SQL)** — Fetches 200,000+ real TTC delay records from Toronto Open Data, joins hourly weather from Open-Meteo API, runs all aggregations as SQL (CTEs, window functions, conditional aggregation) via DuckDB.

2. **ML model (Python)** — Weather-driven reliability degradation classifier. Predicts whether a route will fall below TTC's 80% on-time target in a 2-hour window, using precipitation, temperature, snowfall, wind, and route-level historical risk statistics as features.

3. **Power BI dashboard (4 pages)** — Executive summary, route deep-dive, weather impact analysis, 7-day risk forecast. Published via UWaterloo Microsoft 365.

---

## Data Sources

| Source | Data |
|---|---|
| City of Toronto Open Data — TTC Bus Delay Data | 200k+ real incident records: route, time, incident type, delay duration |
| Open-Meteo Historical Weather API | Hourly Toronto weather: temp, precipitation, snowfall, wind, WMO condition codes |
| Open-Meteo Forecast API | 7-day hourly forecast for the risk prediction page |

All data is real and public. No synthetic data.

---

## SQL Highlights

All aggregations are in `sql/transform.sql` using DuckDB. Key patterns:

```sql
-- 7-day rolling on-time average per route
AVG(on_time_pct) OVER (
    PARTITION BY route
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7d_on_time_pct

-- Route risk score from normalized metrics
0.35 * sig_norm + 0.30 * mean_norm +
0.20 * p95_norm + 0.15 * vol_norm AS risk_score

-- Weather impact: delay uplift under rain
AVG(CASE WHEN is_precipitation = 1 THEN delay_min ELSE NULL END) -
AVG(CASE WHEN is_precipitation = 0 THEN delay_min ELSE NULL END)
    AS weather_sensitivity_precip
```

---

## Run Locally

```bash
pip install -r requirements.txt
python pipeline/run_pipeline.py
```

Outputs 6 Excel files to `data/output/` ready for Power BI. First run fetches data (~3 min). Subsequent runs use cache (~30 sec).

See `POWERBI_SETUP.md` for the full dashboard build instructions.

---

## Project Structure

```
ttc-analytics/
├── pipeline/
│   ├── fetch_data.py       # TTC + weather data fetching
│   ├── model.py            # Degradation classifier + forecast
│   └── run_pipeline.py     # Master runner
├── sql/
│   └── transform.sql       # All SQL transformations (DuckDB)
├── data/
│   └── output/             # Excel files for Power BI (auto-created)
├── models/                 # Saved model (auto-created)
├── requirements.txt
├── POWERBI_SETUP.md        # Step-by-step Power BI instructions
└── README.md
```