-- ============================================================================
-- sql/schema/03_warehouse_layer.sql
-- Warehouse Layer: Star Schema for Analytics
-- ============================================================================

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key SERIAL PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_key SERIAL PRIMARY KEY,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    depth_category VARCHAR(20),
    region VARCHAR(100),
    place TEXT,
    UNIQUE(latitude, longitude, place)
);

-- Dimension: Event Type
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type_key SERIAL PRIMARY KEY,
    magnitude_type VARCHAR(10) NOT NULL UNIQUE,
    magnitude_category VARCHAR(20),
    description TEXT
);

-- Fact: Earthquake Events
CREATE TABLE IF NOT EXISTS fact_earthquake_events (
    fact_key BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    time_key INTEGER REFERENCES dim_time(time_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    event_type_key INTEGER REFERENCES dim_event_type(event_type_key),
    magnitude DECIMAL(3,2),
    depth DECIMAL(8,3),
    significance INTEGER,
    tsunami BOOLEAN,
    status VARCHAR(20),
    event_time TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_time ON fact_earthquake_events(time_key);
CREATE INDEX idx_fact_location ON fact_earthquake_events(location_key);
CREATE INDEX idx_fact_event_type ON fact_earthquake_events(event_type_key);
CREATE INDEX idx_fact_magnitude ON fact_earthquake_events(magnitude);
CREATE INDEX idx_fact_event_time ON fact_earthquake_events(event_time);