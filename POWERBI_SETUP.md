# Power BI Dashboard Setup Guide
## TTC Transit Reliability Analytics

---

## Step 0 — Run the pipeline first

```powershell
cd ttc-analytics
pip install -r requirements.txt
python pipeline/run_pipeline.py
```

This creates `data/output/` with 6 Excel files. Takes ~3 minutes on first run
(fetching data + training model). Subsequent runs use cache and take ~30 seconds.

---

## Step 1 — Connect Power BI to the data files

1. Open **Power BI Desktop**
2. **Home → Get Data → Excel Workbook**
3. Load each file from `data/output/`:

| File | Table Name in Power BI |
|---|---|
| `route_daily.xlsx` | `RouteDaily` |
| `route_hourly.xlsx` | `RouteHourly` |
| `weather_impact.xlsx` | `WeatherImpact` |
| `network_summary.xlsx` | `NetworkSummary` |
| `route_risk_scores.xlsx` | `RouteRiskScores` |
| `tomorrow_forecast.xlsx` | `Forecast` |

---

## Step 2 — Create relationships (Model view)

In the **Model** view, create these relationships:

- `RouteDaily[route]` → `RouteRiskScores[route]` (Many to One)
- `RouteHourly[route]` → `RouteRiskScores[route]` (Many to One)
- `Forecast[route]` → `RouteRiskScores[route]` (Many to One)

---

## Step 3 — Create DAX measures

In the **Data** view, select `RouteDaily` and add these measures:

```dax
// Network on-time rate
Network On-Time % =
AVERAGE(RouteDaily[on_time_pct])

// Total delay hours
Total Delay Hours =
SUM(RouteDaily[total_delay_hrs])

// Routes below 80% target
Routes Below Target =
CALCULATE(
    DISTINCTCOUNT(RouteDaily[route]),
    RouteDaily[on_time_pct] < 80
)

// Weather impact: delay uplift on rainy days vs clear
Rain Delay Uplift =
VAR rain_avg =
    CALCULATE(
        AVERAGE(RouteDaily[avg_delay_min]),
        RouteDaily[had_precipitation] = 1
    )
VAR clear_avg =
    CALCULATE(
        AVERAGE(RouteDaily[avg_delay_min]),
        RouteDaily[had_precipitation] = 0
    )
RETURN
    DIVIDE(rain_avg - clear_avg, clear_avg, 0)

// Snow impact
Snow Delay Uplift =
VAR snow_avg =
    CALCULATE(
        AVERAGE(RouteDaily[avg_delay_min]),
        RouteDaily[had_snow] = 1
    )
VAR clear_avg =
    CALCULATE(
        AVERAGE(RouteDaily[avg_delay_min]),
        RouteDaily[had_precipitation] = 0,
        RouteDaily[had_snow] = 0
    )
RETURN
    DIVIDE(snow_avg - clear_avg, clear_avg, 0)

// Worst routes today (dynamic)
High Risk Route Count =
CALCULATE(
    DISTINCTCOUNT(RouteRiskScores[route]),
    RouteRiskScores[risk_tier] IN {"Critical", "High"}
)
```

---

## Step 4 — Build the four dashboard pages

---

### PAGE 1: Executive Summary

**Purpose:** One-glance network health overview for a transit manager.

**Visuals to add:**

1. **KPI Cards** (top row, 4 cards)
   - Network On-Time % → `[Network On-Time %]`
   - Total Delay Hours → `[Total Delay Hours]`
   - Routes Below 80% Target → `[Routes Below Target]`
   - Rain Delay Uplift → `[Rain Delay Uplift]` (format as %)

2. **Line chart** — Network on-time % over time
   - X axis: `NetworkSummary[date]`
   - Y axis: `NetworkSummary[network_on_time_pct]`
   - Secondary line: `NetworkSummary[rolling_30d_on_time_pct]`
   - Title: "Network On-Time Performance — 30-Day Rolling Average"

3. **Bar chart** — Top 10 worst routes by risk score
   - Y axis: `RouteRiskScores[route]`
   - X axis: `RouteRiskScores[risk_score]`
   - Color by: `RouteRiskScores[risk_tier]`
     (Critical=dark red, High=orange, Medium=amber, Low=green)
   - Title: "Highest Risk Routes"

4. **Clustered bar** — Monthly total delay hours
   - X axis: `NetworkSummary[year_month]`
   - Y axis: `NetworkSummary[total_delay_hrs]`
   - Title: "Total Delay Hours by Month"

5. **Slicer** — Date range picker
   - Field: `NetworkSummary[date]`
   - Style: Between

---

### PAGE 2: Route Deep Dive

**Purpose:** Drill into any individual route.

**Visuals:**

1. **Slicer** — Route selector
   - Field: `RouteDaily[route]`
   - Style: Dropdown

2. **KPI Cards** (for selected route)
   - Avg Delay: `AVERAGE(RouteDaily[avg_delay_min])`
   - On-Time %: `AVERAGE(RouteDaily[on_time_pct])`
   - P95 Delay: `AVERAGE(RouteDaily[p95_delay_min])`
   - Weather Sensitivity: from `RouteRiskScores[weather_sensitivity_precip]`

3. **Line chart** — Route on-time % over time
   - X: `RouteDaily[date]`
   - Y: `RouteDaily[on_time_pct]`, `RouteDaily[rolling_7d_on_time_pct]`
   - Add reference line at Y=80 (TTC target)

4. **Matrix heatmap** — Avg delay by hour and day
   - Rows: `RouteHourly[time_period]`
   - Columns: `RouteHourly[hour]`
   - Values: `RouteHourly[avg_delay_min]`
   - Conditional formatting: white→amber→red

5. **Bar chart** — Incident category breakdown
   - X: `RouteDaily` incident columns (mechanical, traffic, passenger, operator)
   - Title: "Incidents by Category"

---

### PAGE 3: Weather Impact Analysis

**Purpose:** Show how weather degrades service — the key analytical insight.

**Visuals:**

1. **Clustered bar** — Avg delay by weather condition
   - Y axis: `WeatherImpact[weather_condition]`
   - X axis: `WeatherImpact[avg_delay_min]`
   - Color by: `WeatherImpact[severity_bucket]`
   - Title: "Average Delay by Weather Condition"

2. **Scatter plot** — Precipitation vs delay
   - X: `RouteDaily[total_precip_mm]`
   - Y: `RouteDaily[avg_delay_min]`
   - Size: `RouteDaily[total_incidents]`
   - Title: "Precipitation vs Average Delay"

3. **Clustered bar** — On-time % by weather severity bucket
   - X: `WeatherImpact[severity_bucket]`
   - Y: `WeatherImpact[on_time_pct]`
   - Add reference line at 80
   - Title: "On-Time Performance by Weather Severity"

4. **Table** — Weather impact summary
   - Columns: condition, avg delay, on-time %, significant delay %, incidents
   - Sort by avg delay descending
   - Conditional formatting on on-time %

5. **KPI cards**
   - Rain Delay Uplift: `[Rain Delay Uplift]`
   - Snow Delay Uplift: `[Snow Delay Uplift]`

---

### PAGE 4: 7-Day Risk Forecast

**Purpose:** Operational forward-looking view. Which routes need attention?

**Visuals:**

1. **Matrix** — Route × Day alert level
   - Rows: `Forecast[route]` (filter top 20 by risk score)
   - Columns: `Forecast[day_name]`
   - Values: `MAX(Forecast[max_alert_score])`
   - Conditional formatting: green→yellow→orange→red
   - Title: "7-Day Route Risk Forecast"

2. **Bar chart** — Routes by alert level for tomorrow
   - Filter: `Forecast[date]` = TODAY()
   - X: `Forecast[alert_level]`
   - Y: `DISTINCTCOUNT(Forecast[route])`

3. **Table** — Top 15 route-day risk combinations
   - Columns: date, day name, route, degradation probability,
     alert level, weather condition, risk tier
   - Sort by degradation_prob descending
   - Conditional format alert level column

4. **Line chart** — Network avg degradation probability over 7 days
   - X: `Forecast[date]`
   - Y: `AVERAGE(Forecast[max_degradation_prob])`

5. **Slicer** — Alert level filter
   - Field: `Forecast[alert_level]`
   - Multi-select

---

## Step 5 — Style the dashboard

**Colour theme:**
- Background: `#FBF6EC` (warm cream)
- Primary accent: `#C8860A` (amber)
- Critical: `#8B1A1A` (dark red)
- High: `#B85C10` (orange)
- Medium: `#C8860A` (amber)
- Low / Good: `#2C6E3F` (green)
- Text: `#1E1408` (near black)

To apply: **View → Themes → Customize current theme** and set these hex values.

**Font:** Segoe UI (default Power BI) is fine — no need to change.

**Page size:** Set to 1440 × 900 for each page (View → Page View → Actual Size).

---

## Step 6 — Publish to Power BI Service

1. **File → Publish → Publish to Power BI**
2. Sign in with your `@uwaterloo.ca` Microsoft account
3. Select **My Workspace**
4. Once published, go to **app.powerbi.com**
5. Find your report → click **Share** → **Anyone with the link can view**
6. Copy the link — this is what goes on your resume and GitHub

---

## Step 7 — Update your GitHub README

After publishing, add the Power BI link to your README.md:

```markdown
🔗 **[Live Power BI Dashboard](your-link-here)**
```

---

## Resume bullets

```
TTC Transit Reliability Analytics | Python, SQL (DuckDB), Power BI,
Scikit-Learn, Open-Meteo API, Toronto Open Data

• Engineered a multi-source data pipeline joining 200,000+ real TTC bus
  delay records with hourly Toronto weather data via Open-Meteo API,
  performing joins, CTEs, and window-function aggregations in SQL (DuckDB)
  to produce route-level reliability metrics across 100+ routes

• Built a weather-driven service degradation model predicting whether a
  route falls below TTC's 80% on-time target in a given 2-hour window,
  using precipitation, temperature, and route-level risk statistics as
  features, achieving OOF AUC of 0.XX on held-out validation windows

• Delivered findings as a published 4-page Power BI dashboard with
  executive KPIs, route drill-down, weather impact analysis, and a
  7-day risk forecast — enabling operational staff to identify at-risk
  routes before service degradation occurs
```