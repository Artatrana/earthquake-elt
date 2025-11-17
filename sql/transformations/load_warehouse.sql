-- ============================================================================
-- sql/transformations/load_warehouse.sql
-- Transform: Staging → Warehouse (Star Schema)
-- ============================================================================

-- Populate time dimension (for date range in staging)
INSERT INTO dim_time (
    date_actual, year, quarter, month, month_name, day,
    day_of_week, day_name, week_of_year, is_weekend
)
SELECT DISTINCT
    event_time::DATE as date_actual,
    EXTRACT(YEAR FROM event_time)::INTEGER as year,
    EXTRACT(QUARTER FROM event_time)::INTEGER as quarter,
    EXTRACT(MONTH FROM event_time)::INTEGER as month,
    TO_CHAR(event_time, 'Month') as month_name,
    EXTRACT(DAY FROM event_time)::INTEGER as day,
    EXTRACT(DOW FROM event_time)::INTEGER as day_of_week,
    TO_CHAR(event_time, 'Day') as day_name,
    EXTRACT(WEEK FROM event_time)::INTEGER as week_of_year,
    EXTRACT(DOW FROM event_time) IN (0, 6) as is_weekend
FROM stg_earthquakes
WHERE NOT EXISTS (
    SELECT 1 FROM dim_time t
    WHERE t.date_actual = event_time::DATE
);

-- Populate location dimension
INSERT INTO dim_location (
    latitude, longitude, depth_category, region, place
)
SELECT DISTINCT
    latitude,
    longitude,
    CASE
        WHEN depth < 70 THEN 'Shallow'
        WHEN depth < 300 THEN 'Intermediate'
        ELSE 'Deep'
    END as depth_category,
    CASE
        WHEN latitude BETWEEN 24 AND 49 AND longitude BETWEEN -125 AND -65
            THEN 'Continental US'
        WHEN latitude BETWEEN 55 AND 72 AND longitude BETWEEN -170 AND -140
            THEN 'Alaska'
        WHEN latitude BETWEEN 18 AND 23 AND longitude BETWEEN -161 AND -154
            THEN 'Hawaii'
        WHEN latitude BETWEEN -90 AND -60
            THEN 'Antarctica'
        ELSE 'Other'
    END as region,
    place
FROM stg_earthquakes
WHERE NOT EXISTS (
    SELECT 1 FROM dim_location l
    WHERE l.latitude = stg_earthquakes.latitude
      AND l.longitude = stg_earthquakes.longitude
      AND COALESCE(l.place, '') = COALESCE(stg_earthquakes.place, '')
);

-- Populate event type dimension
INSERT INTO dim_event_type (
    magnitude_type, magnitude_category, description
)
SELECT DISTINCT
    magnitude_type,
    CASE
        WHEN magnitude_type IN ('mb', 'mb_lg') THEN 'Body Wave'
        WHEN magnitude_type IN ('ms', 'ms_20') THEN 'Surface Wave'
        WHEN magnitude_type IN ('mw', 'mww', 'mwc', 'mwb') THEN 'Moment'
        WHEN magnitude_type IN ('ml', 'mlg') THEN 'Local'
        WHEN magnitude_type IN ('md') THEN 'Duration'
        ELSE 'Other'
    END as magnitude_category,
    CASE
        WHEN magnitude_type IN ('mb', 'mb_lg') THEN 'Body wave magnitude'
        WHEN magnitude_type IN ('ms', 'ms_20') THEN 'Surface wave magnitude'
        WHEN magnitude_type IN ('mw', 'mww', 'mwc', 'mwb') THEN 'Moment magnitude'
        WHEN magnitude_type IN ('ml', 'mlg') THEN 'Local (Richter) magnitude'
        WHEN magnitude_type IN ('md') THEN 'Duration magnitude'
        ELSE 'Other magnitude type'
    END as description
FROM stg_earthquakes
WHERE magnitude_type IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dim_event_type e
    WHERE e.magnitude_type = stg_earthquakes.magnitude_type
);

-- Populate fact table
INSERT INTO fact_earthquake_events (
    event_id, time_key, location_key, event_type_key,
    magnitude, depth, significance, tsunami, status, event_time
)
SELECT
    se.event_id,
    dt.time_key,
    dl.location_key,
    det.event_type_key,
    se.magnitude,
    se.depth,
    se.significance,
    se.tsunami,
    se.status,
    se.event_time
FROM stg_earthquakes se
LEFT JOIN dim_time dt ON dt.date_actual = se.event_time::DATE
LEFT JOIN dim_location dl ON
    dl.latitude = se.latitude
    AND dl.longitude = se.longitude
    AND COALESCE(dl.place, '') = COALESCE(se.place, '')
LEFT JOIN dim_event_type det ON det.magnitude_type = se.magnitude_type
WHERE NOT EXISTS (
    SELECT 1 FROM fact_earthquake_events f
    WHERE f.event_id = se.event_id
);
