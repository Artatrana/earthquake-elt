-- ============================================================================
-- sql/transformations/load_staging.sql
-- Transform: Raw → Staging
-- ============================================================================

-- Load staging from raw layer (incremental)
INSERT INTO stg_earthquakes (
    event_id,
    magnitude,
    magnitude_type,
    place,
    event_time,
    updated_time,
    latitude,
    longitude,
    depth,
    status,
    tsunami,
    significance,
    source_batch_id
)
SELECT DISTINCT
    raw_data->>'id' as event_id,
    NULLIF(raw_data->'properties'->>'mag', '')::DECIMAL(3,2) as magnitude,
    raw_data->'properties'->>'magType' as magnitude_type,
    raw_data->'properties'->>'place' as place,
    TO_TIMESTAMP((raw_data->'properties'->>'time')::BIGINT / 1000.0) as event_time,
    TO_TIMESTAMP((raw_data->'properties'->>'updated')::BIGINT / 1000.0) as updated_time,
    (raw_data->'geometry'->'coordinates'->>1)::DECIMAL(9,6) as latitude,
    (raw_data->'geometry'->'coordinates'->>0)::DECIMAL(9,6) as longitude,
    (raw_data->'geometry'->'coordinates'->>2)::DECIMAL(8,3) as depth,
    raw_data->'properties'->>'status' as status,
    COALESCE((raw_data->'properties'->>'tsunami')::INTEGER::BOOLEAN, FALSE) as tsunami,
    (raw_data->'properties'->>'sig')::INTEGER as significance,
    batch_id as source_batch_id
FROM raw_earthquake_events
WHERE NOT EXISTS (
    SELECT 1 FROM stg_earthquakes s
    WHERE s.event_id = raw_earthquake_events.raw_data->>'id'
)
AND raw_data->'properties'->>'mag' IS NOT NULL
AND raw_data->'geometry'->'coordinates' IS NOT NULL;
