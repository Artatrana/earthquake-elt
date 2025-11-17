-- ============================================================================
-- sql/schema/02_staging_layer.sql
-- Staging Layer: Normalized and cleansed data
-- ============================================================================

-- Staging: Cleansed earthquake events
CREATE TABLE IF NOT EXISTS stg_earthquakes (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    magnitude DECIMAL(3,2),
    magnitude_type VARCHAR(10),
    place TEXT,
    event_time TIMESTAMP NOT NULL,
    updated_time TIMESTAMP,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    depth DECIMAL(8,3),
    status VARCHAR(20),
    tsunami BOOLEAN,
    significance INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_batch_id UUID
);

CREATE INDEX idx_stg_earthquakes_time ON stg_earthquakes(event_time);
CREATE INDEX idx_stg_earthquakes_magnitude ON stg_earthquakes(magnitude);
CREATE INDEX idx_stg_earthquakes_location ON stg_earthquakes(latitude, longitude);
