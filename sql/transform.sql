-- ============================================================
-- transform.sql — TTC Transit Reliability Analytics
-- DuckDB SQL: CTEs, window functions, conditional aggregation
-- ============================================================


-- ============================================================
-- TABLE 1: route_daily
-- One row per (route, date): reliability metrics + weather
-- ============================================================
CREATE OR REPLACE TABLE route_daily AS
WITH daily_base AS (
    SELECT
        route,
        CAST(date AS DATE)                          AS date,
        MAX(is_weekend)                             AS is_weekend,
        COUNT(*)                                    AS total_incidents,
        SUM(is_significant)                         AS significant_delays,
        SUM(is_severe)                              AS severe_delays,
        SUM(is_on_time)                             AS on_time_count,
        ROUND(AVG(delay_min), 2)                    AS avg_delay_min,
        ROUND(MEDIAN(delay_min), 2)                 AS median_delay_min,
        ROUND(MAX(delay_min), 1)                    AS max_delay_min,
        ROUND(QUANTILE_CONT(delay_min, 0.95), 1)    AS p95_delay_min,
        ROUND(SUM(delay_min) / 60.0, 2)             AS total_delay_hrs,
        ROUND(AVG(is_on_time) * 100, 2)             AS on_time_pct,
        ROUND(AVG(is_significant) * 100, 2)         AS significant_delay_pct,
        ROUND(AVG(temp_c), 1)                       AS avg_temp_c,
        ROUND(SUM(precip_mm), 1)                    AS total_precip_mm,
        ROUND(SUM(snow_cm), 1)                      AS total_snow_cm,
        ROUND(AVG(wind_kph), 1)                     AS avg_wind_kph,
        ROUND(MAX(weather_severity), 0)             AS max_weather_severity,
        MAX(is_precipitation)                       AS had_precipitation,
        MAX(is_snow)                                AS had_snow,
        MAX(is_extreme_cold)                        AS had_extreme_cold,
        COUNT(CASE WHEN incident_category = 'Mechanical' THEN 1 END) AS mechanical_incidents,
        COUNT(CASE WHEN incident_category = 'Traffic'    THEN 1 END) AS traffic_incidents,
        COUNT(CASE WHEN incident_category = 'Passenger'  THEN 1 END) AS passenger_incidents,
        COUNT(CASE WHEN incident_category = 'Operator'   THEN 1 END) AS operator_incidents
    FROM delays
    GROUP BY route, CAST(date AS DATE)
),

-- Derive date labels AFTER grouping to avoid DuckDB column ambiguity
labelled AS (
    SELECT
        *,
        strftime('%Y-%m', date)  AS year_month,
        DAYOFWEEK(date)          AS day_of_week,
        DAYNAME(date)            AS day_name
    FROM daily_base
),

-- 7-day rolling window functions
rolling AS (
    SELECT
        *,
        ROUND(
            AVG(on_time_pct) OVER (
                PARTITION BY route
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_7d_on_time_pct,
        ROUND(
            AVG(avg_delay_min) OVER (
                PARTITION BY route
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_7d_avg_delay,
        RANK() OVER (
            PARTITION BY date
            ORDER BY on_time_pct ASC
        ) AS daily_reliability_rank
    FROM labelled
)

SELECT * FROM rolling
ORDER BY route, date;


-- ============================================================
-- TABLE 2: route_hourly
-- Avg delay and on-time rate by (route, hour) — heatmap data
-- ============================================================
CREATE OR REPLACE TABLE route_hourly AS
SELECT
    route,
    hour,
    time_period,
    is_rush_hour,
    COUNT(*)                                AS total_incidents,
    ROUND(AVG(delay_min), 2)                AS avg_delay_min,
    ROUND(MEDIAN(delay_min), 2)             AS median_delay_min,
    ROUND(AVG(is_on_time) * 100, 2)         AS on_time_pct,
    ROUND(AVG(is_significant) * 100, 2)     AS significant_delay_pct,
    ROUND(SUM(delay_min) / 60.0, 2)         AS total_delay_hrs,
    ROUND(AVG(precip_mm), 2)                AS avg_precip_mm,
    ROUND(AVG(CASE WHEN is_precipitation = 1 THEN delay_min ELSE NULL END), 2) AS avg_delay_wet,
    ROUND(AVG(CASE WHEN is_precipitation = 0 THEN delay_min ELSE NULL END), 2) AS avg_delay_dry
FROM delays
GROUP BY route, hour, time_period, is_rush_hour
ORDER BY route, hour;


-- ============================================================
-- TABLE 3: weather_impact
-- How weather degrades service — key analytical insight
-- ============================================================
CREATE OR REPLACE TABLE weather_impact AS
WITH base AS (
    SELECT
        weather_condition,
        CASE
            WHEN weather_severity = 0             THEN 'Clear / Mild'
            WHEN weather_severity BETWEEN 1 AND 2 THEN 'Light Adverse'
            WHEN weather_severity BETWEEN 3 AND 4 THEN 'Moderate Adverse'
            ELSE                                       'Severe Adverse'
        END                                         AS severity_bucket,
        delay_min,
        is_on_time,
        is_significant,
        is_severe,
        temp_c,
        precip_mm,
        snow_cm
    FROM delays
    WHERE weather_condition IS NOT NULL
      AND weather_condition != 'Unknown'
)

SELECT
    weather_condition,
    severity_bucket,
    COUNT(*)                                AS total_incidents,
    ROUND(AVG(delay_min), 2)                AS avg_delay_min,
    ROUND(MEDIAN(delay_min), 2)             AS median_delay_min,
    ROUND(QUANTILE_CONT(delay_min, 0.95), 1) AS p95_delay_min,
    ROUND(AVG(is_on_time) * 100, 2)         AS on_time_pct,
    ROUND(AVG(is_significant) * 100, 2)     AS significant_delay_pct,
    ROUND(AVG(is_severe) * 100, 2)          AS severe_delay_pct,
    ROUND(AVG(temp_c), 1)                   AS avg_temp_c,
    ROUND(AVG(precip_mm), 2)                AS avg_precip_mm,
    ROUND(AVG(snow_cm), 2)                  AS avg_snow_cm
FROM base
GROUP BY weather_condition, severity_bucket
ORDER BY avg_delay_min DESC;


-- ============================================================
-- TABLE 4: network_summary
-- Daily network-wide KPIs with 30-day rolling averages
-- ============================================================
CREATE OR REPLACE TABLE network_summary AS
WITH daily_network AS (
    SELECT
        CAST(date AS DATE)                          AS date,
        MAX(is_weekend)                             AS is_weekend,
        COUNT(*)                                    AS total_incidents,
        COUNT(DISTINCT route)                       AS routes_affected,
        ROUND(AVG(delay_min), 2)                    AS avg_delay_min,
        ROUND(AVG(is_on_time) * 100, 2)             AS network_on_time_pct,
        ROUND(AVG(is_significant) * 100, 2)         AS network_significant_pct,
        ROUND(SUM(delay_min) / 60.0, 1)             AS total_delay_hrs,
        ROUND(AVG(temp_c), 1)                       AS avg_temp_c,
        ROUND(SUM(precip_mm), 1)                    AS total_precip_mm,
        ROUND(MAX(weather_severity), 0)             AS max_weather_severity,
        MAX(is_snow)                                AS had_snow,
        MAX(is_extreme_cold)                        AS had_extreme_cold
    FROM delays
    GROUP BY CAST(date AS DATE)
),

-- Derive labels after grouping
labelled AS (
    SELECT
        *,
        strftime('%Y-%m', date) AS year_month,
        DAYNAME(date)           AS day_name
    FROM daily_network
),

with_rolling AS (
    SELECT
        *,
        ROUND(
            AVG(network_on_time_pct) OVER (
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_30d_on_time_pct,
        ROUND(
            AVG(avg_delay_min) OVER (
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_30d_avg_delay
    FROM labelled
)

SELECT * FROM with_rolling ORDER BY date;


-- ============================================================
-- TABLE 5: route_risk_scores
-- Composite risk score per route with weather sensitivity
-- ============================================================
CREATE OR REPLACE TABLE route_risk_scores AS
WITH route_stats AS (
    SELECT
        route,
        COUNT(*)                                    AS total_incidents,
        ROUND(AVG(delay_min), 2)                    AS mean_delay_min,
        ROUND(MEDIAN(delay_min), 2)                 AS median_delay_min,
        ROUND(QUANTILE_CONT(delay_min, 0.95), 1)    AS p95_delay_min,
        ROUND(AVG(is_on_time) * 100, 2)             AS on_time_pct,
        ROUND(AVG(is_significant) * 100, 2)         AS significant_delay_pct,
        ROUND(AVG(is_severe) * 100, 2)              AS severe_delay_pct,
        ROUND(SUM(delay_min) / 60.0, 1)             AS total_delay_hrs,
        ROUND(
            AVG(CASE WHEN is_precipitation = 1 THEN delay_min ELSE NULL END) -
            AVG(CASE WHEN is_precipitation = 0 THEN delay_min ELSE NULL END),
        2) AS weather_sensitivity_precip,
        ROUND(
            AVG(CASE WHEN is_snow = 1 THEN delay_min ELSE NULL END) -
            AVG(CASE WHEN is_snow = 0 THEN delay_min ELSE NULL END),
        2) AS weather_sensitivity_snow,
        ROUND(
            AVG(CASE WHEN is_rush_hour = 1 THEN delay_min ELSE NULL END) -
            AVG(CASE WHEN is_rush_hour = 0 THEN delay_min ELSE NULL END),
        2) AS rush_hour_sensitivity
    FROM delays
    GROUP BY route
    HAVING COUNT(*) >= 10
),

normalized AS (
    SELECT
        *,
        (significant_delay_pct - MIN(significant_delay_pct) OVER()) /
            NULLIF(MAX(significant_delay_pct) OVER() - MIN(significant_delay_pct) OVER(), 0)
            AS sig_norm,
        (mean_delay_min - MIN(mean_delay_min) OVER()) /
            NULLIF(MAX(mean_delay_min) OVER() - MIN(mean_delay_min) OVER(), 0)
            AS mean_norm,
        (p95_delay_min - MIN(p95_delay_min) OVER()) /
            NULLIF(MAX(p95_delay_min) OVER() - MIN(p95_delay_min) OVER(), 0)
            AS p95_norm,
        (total_incidents - MIN(total_incidents) OVER()) /
            NULLIF(MAX(total_incidents) OVER() - MIN(total_incidents) OVER(), 0)
            AS vol_norm
    FROM route_stats
)

SELECT
    route,
    total_incidents,
    mean_delay_min,
    median_delay_min,
    p95_delay_min,
    on_time_pct,
    significant_delay_pct,
    severe_delay_pct,
    total_delay_hrs,
    COALESCE(weather_sensitivity_precip, 0) AS weather_sensitivity_precip,
    COALESCE(weather_sensitivity_snow, 0)   AS weather_sensitivity_snow,
    COALESCE(rush_hour_sensitivity, 0)      AS rush_hour_sensitivity,
    ROUND(
        0.35 * COALESCE(sig_norm, 0) +
        0.30 * COALESCE(mean_norm, 0) +
        0.20 * COALESCE(p95_norm, 0) +
        0.15 * COALESCE(vol_norm, 0),
    4) AS risk_score,
    CASE
        WHEN (0.35 * COALESCE(sig_norm, 0) + 0.30 * COALESCE(mean_norm, 0) +
              0.20 * COALESCE(p95_norm, 0) + 0.15 * COALESCE(vol_norm, 0)) > 0.75 THEN 'Critical'
        WHEN (0.35 * COALESCE(sig_norm, 0) + 0.30 * COALESCE(mean_norm, 0) +
              0.20 * COALESCE(p95_norm, 0) + 0.15 * COALESCE(vol_norm, 0)) > 0.55 THEN 'High'
        WHEN (0.35 * COALESCE(sig_norm, 0) + 0.30 * COALESCE(mean_norm, 0) +
              0.20 * COALESCE(p95_norm, 0) + 0.15 * COALESCE(vol_norm, 0)) > 0.35 THEN 'Medium'
        ELSE 'Low'
    END AS risk_tier,
    RANK() OVER (ORDER BY
        0.35 * COALESCE(sig_norm, 0) + 0.30 * COALESCE(mean_norm, 0) +
        0.20 * COALESCE(p95_norm, 0) + 0.15 * COALESCE(vol_norm, 0)
        DESC) AS risk_rank
FROM normalized
ORDER BY risk_score DESC;