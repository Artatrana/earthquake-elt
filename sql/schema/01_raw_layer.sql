-- ============================================================================
-- sql/schema/01_raw_layer.sql
-- Raw Layer: Append-only tables written by ingestion process
-- ============================================================================

-- Raw earthquake events (immutable JSONB storage)
CREATE TABLE IF NOT EXISTS raw_earthquake_events (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    event_id VARCHAR(50) NOT NULL,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_id, batch_id)
);

CREATE INDEX idx_raw_events_batch ON raw_earthquake_events(batch_id);
CREATE INDEX idx_raw_events_ingested ON raw_earthquake_events(ingested_at);
CREATE INDEX idx_raw_events_event_id ON raw_earthquake_events(event_id);

-- Ingestion metadata for monitoring
CREATE TABLE IF NOT EXISTS ingestion_metadata (
    id SERIAL PRIMARY KEY,
    batch_id UUID UNIQUE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_fetched INTEGER,
    records_inserted INTEGER,
    status VARCHAR(20),
    error_message TEXT
);

-- Error tracking
CREATE TABLE IF NOT EXISTS ingestion_errors (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    event_id VARCHAR(50),
    error_type VARCHAR(50),
    error_message TEXT,
    raw_data JSONB,
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_errors_batch ON ingestion_errors(batch_id);
CREATE INDEX idx_errors_occurred ON ingestion_errors(occurred_at);