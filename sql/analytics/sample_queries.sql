-- ============================================================================
-- sql/analytics/sample_queries.sql
-- Sample Analytical Queries
-- ============================================================================

-- Query 1: Magnitude distribution by region
SELECT
    dl.region,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    ROUND(MAX(f.magnitude), 2) as max_magnitude,
    ROUND(MIN(f.magnitude), 2) as min_magnitude,
    SUM(CASE WHEN f.tsunami THEN 1 ELSE 0 END) as tsunami_events
FROM fact_earthquake_events f
JOIN dim_location dl ON f.location_key = dl.location_key
GROUP BY dl.region
ORDER BY event_count DESC;

-- Query 2: Temporal patterns - events by day of week
SELECT
    dt.day_name,
    dt.is_weekend,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    SUM(CASE WHEN f.magnitude >= 5.0 THEN 1 ELSE 0 END) as major_events
FROM fact_earthquake_events f
JOIN dim_time dt ON f.time_key = dt.time_key
GROUP BY dt.day_name, dt.is_weekend, dt.day_of_week
ORDER BY dt.day_of_week;

-- Query 3: Significant events by depth category
SELECT
    dl.depth_category,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    ROUND(AVG(f.significance), 0) as avg_significance,
    COUNT(CASE WHEN f.magnitude >= 6.0 THEN 1 END) as strong_events
FROM fact_earthquake_events f
JOIN dim_location dl ON f.location_key = dl.location_key
WHERE f.significance > 100
GROUP BY dl.depth_category
ORDER BY avg_significance DESC;

-- Query 4: Event trends over time (monthly aggregation)
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    MAX(f.magnitude) as max_magnitude,
    SUM(f.significance) as total_significance
FROM fact_earthquake_events f
JOIN dim_time dt ON f.time_key = dt.time_key
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- Query 5: Magnitude type analysis
SELECT
    det.magnitude_type,
    det.magnitude_category,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    ROUND(STDDEV(f.magnitude), 2) as stddev_magnitude,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.magnitude) as median_magnitude
FROM fact_earthquake_events f
JOIN dim_event_type det ON f.event_type_key = det.event_type_key
GROUP BY det.magnitude_type, det.magnitude_category
ORDER BY event_count DESC;

-- Query 6: High-impact events (recent, strong, with tsunami potential)
SELECT
    f.event_id,
    dt.date_actual,
    dl.place,
    dl.region,
    f.magnitude,
    f.depth,
    dl.depth_category,
    f.tsunami,
    f.significance,
    det.magnitude_type
FROM fact_earthquake_events f
JOIN dim_time dt ON f.time_key = dt.time_key
JOIN dim_location dl ON f.location_key = dl.location_key
JOIN dim_event_type det ON f.event_type_key = det.event_type_key
WHERE f.magnitude >= 5.0
  AND f.event_time >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY f.magnitude DESC, f.significance DESC
LIMIT 20;
