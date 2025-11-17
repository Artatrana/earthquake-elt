# Earthquake ELT Pipeline - Complete Project

A ELT system that ingests earthquake events from the USGS Earthquake Catalog API and models them into a local analytics data warehouse with a star schema.

## 🎯 Design Overview

### Architecture

```
┌─────────────────┐
│   USGS API      │
│  (GeoJSON)      │
└────────┬────────┘
         │ Extract  
         ↓
┌─────────────────┐
│   Raw Layer     │  ← Append-only JSONB storage
│  (PostgreSQL)   │  ← Metadata & error tracking
└────────┬────────┘
         │ Transform 
         ↓
┌─────────────────┐
│ Staging Layer   │  ← Normalized & cleansed
└────────┬────────┘
         │ Transform (SQL)
         ↓
┌─────────────────┐
│  Star Schema    │  ← 3 Dimensions + 1 Fact
└─────────────────┘
```

## 🚀 Quick Start 

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- 2GB free disk space

### Setup & Run

#### Make sure .env file has DB_PASSWORD defined - you can define your own

```bash
# 1. Complete setup (installs deps, starts DB, initializes schema)
make setup

# 2. Run the pipeline
make run
```

The pipeline will:
1. Fetch earthquake data from USGS API (last 7 days)
2. Validate and load to raw layer
3. Transform through staging to warehouse
4. Display summary statistics

### Sample Output

```
================================================================================
Running FULL PIPELINE
================================================================================

2024-01-15 10:30:01 - Pipeline initialized
2024-01-15 10:30:02 - Fetching events from 2024-01-08 to 2024-01-15
2024-01-15 10:30:15 - Fetched 1247 events from API
2024-01-15 10:30:16 - Validation: 1245 valid, 2 invalid
2024-01-15 10:30:17 - Loaded 1245 events to raw layer
2024-01-15 10:30:18 - Transforming raw → staging
2024-01-15 10:30:19 - Transforming staging → warehouse

================================================================================
PIPELINE SUMMARY
================================================================================
Status: success

Data Period:
  - Start Date: 2025-11-10
  - End Date: 2025-11-17
  
Ingestion:
  - Events fetched: 1247
  - Events valid: 1245
  - Events invalid: 2
  - Events loaded: 1245

Warehouse Counts:
  - Raw events: 1245
  - Staging events: 1245
  - Fact events: 1245
  - Dim time: 8
  - Dim location: 456
  - Dim event type: 12

Duration: 18.45 seconds
================================================================================
```

## 📊 Sample Analytical Queries

```bash
# Run all sample queries
make query
```

### Query Examples

**1. Magnitude Distribution by Region**
```sql
SELECT 
    dl.region,
    COUNT(*) as event_count,
    ROUND(AVG(f.magnitude), 2) as avg_magnitude,
    ROUND(MAX(f.magnitude), 2) as max_magnitude
FROM fact_earthquake_events f
JOIN dim_location dl ON f.location_key = dl.location_key
GROUP BY dl.region
ORDER BY event_count DESC;
```

**Output:**
```
      region      | event_count | avg_magnitude | max_magnitude
------------------+-------------+---------------+---------------
 Continental US   |         567 |          2.34 |          5.8
 Alaska           |         234 |          2.89 |          6.2
 Hawaii           |          89 |          2.12 |          4.3
```

More queries in `sql/analytics/sample_queries.sql`

## 🛠️ Usage

### Basic Commands

```bash
make       # Initial setup
make run        # Run full pipeline
make ingest     # Ingestion only
make transform  # Transformations only
make query      # Run sample queries
make clean      # Clean up
```

### Advanced Usage

```bash
# Custom date range
python run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-31

# Last 30 days
python run_pipeline.py --days 30

# Ingestion only
python run_pipeline.py --ingestion-only

# Transformations only
python run_pipeline.py --transform-only
```

## 🏗️ Implementation Details

### Python Components 

#### 1. API Client (`src/ingestion/api_client.py`)
**Production Features Implemented:**
- ✅ Rate limiting (60 requests/minute)
- ✅ Retry logic with exponential backoff (3 attempts)
- ✅ Pagination support for large datasets
- ✅ Request/response logging
- ✅ Timeout handling

#### 2. Data Validation (`src/ingestion/validators.py`)
- ✅ Pydantic schema validation
- ✅ Range checks (magnitude: 0-10, depth: -10 to 800km)
- ✅ Required field validation
- ✅ Coordinate validation (lat/lon bounds)

#### 3. Error Handling (`src/ingestion/error_handler.py`)
- ✅ Database error logging
- ✅ Error threshold enforcement
- ✅ Error classification

#### 4. Raw Loader (`src/ingestion/loader.py`)
- ✅ Bulk inserts (batch size: 1000)
- ✅ Transaction management
- ✅ Metadata tracking

### SQL Components

#### Database Schema

**Raw Layer:**
- `raw_earthquake_events` - Immutable JSONB storage
- `ingestion_metadata` - Batch tracking
- `ingestion_errors` - Error logging

**Staging Layer:**
- `stg_earthquakes` - Normalized events

**Warehouse (Star Schema):**
- `dim_time` - Time dimension
- `dim_location` - Geographic dimension
- `dim_event_type` - Event classification dimension
- `fact_earthquake_events` - Fact table

All SQL files are in `sql/` directory with clear organization.

## 🛡️ Production Considerations

### Resilience & Fault Tolerance

1. **Retry Logic**: 3 attempts with exponential backoff
2. **Transaction Safety**: All inserts wrapped in transactions
3. **Error Tracking**: Failed records logged to database
4. **Validation Gates**: Data validated before loading

**🔄Future Production Enhancements (documented but not implemented):**
- Dead letter queue for persistent failures
- Circuit breaker pattern
- Graceful degradation
- Automated batch retry mechanism

### Performance

**✅ Implemented:**
1. **Bulk Operations**: 1000 records per insert
2. **Connection Pooling**: PostgreSQL connection pool
3. **Indexed Tables**: All foreign keys and common queries indexed
4. **JSONB Storage**: Efficient semi-structured data storage

**🔄 Future Optimizations (documented):**
- Parallel processing for date ranges
- Table partitioning by date
- Materialized views for aggregations
- Query result caching

### Monitoring

**✅ Implemented:**
1. **Structured Logging**: JSON format to files + console
2. **Metadata Tracking**: Every batch logged with stats
3. **Error Logging**: All failures captured
4. **Duration Metrics**: Pipeline timing tracked

**🔄 Monitoring (integration points documented):**
```python
# Metrics to track:
- API response time (p50, p95, p99)
- Records processed per minute
- Error rate percentage
- Database connection pool utilization

# Alerting thresholds:
- Error rate > 5%
- API latency > 10s
- Pipeline duration > 2x baseline

# Integration examples:
- Prometheus for metrics collection
- Grafana for dashboards
- PagerDuty for alerting
```

### Deployment
**Current:** Local with Docker Compose

**Production Strategy (documented in code):**
Kubernetes 

**Considerations:**
- Orchestration: Airflow/Prefect/Dagster
- Database: Managed PostgreSQL (RDS, Cloud SQL)
- Secrets: Vault, GCP Secrets Manager
- CI/CD: GitHub Actions with automated testing

## 🧪 Testing

```bash
# Run tests
make test

# With coverage
pytest tests/ --cov=src --cov-report=html
```

**Test Structure:**
- `test_api_client.py` - API client with mocked requests
- `test_validators.py` - Validation logic
- `test_integration.py` - End-to-end pipeline test

## 🔍 Troubleshooting

### Common Issues

**Database won't start:**
```bash
docker-compose down -v
docker-compose up -d
sleep 10  # Wait for initialization
```

**API rate limiting:**
Edit `config/config.toml`:
```toml
[api]
rate_limit_per_minute = 30  # Lower rate
```

**View errors:**
```sql
SELECT * FROM ingestion_errors 
ORDER BY occurred_at DESC 
LIMIT 10;
```

### Key Decisions

1. **PostgreSQL** over DuckDB - Better for production, native JSONB
2. **JSONB for raw** - Preserves all API data, schema flexibility
3. **Pydantic validation** - Type safety, clear errors
4. **Tenacity for retries** - Industry standard, configurable
5. **Star schema** - Optimized for analytical queries

### What's Implemented vs Future Enhancement

**✅ Implemented:**
- Complete API client with production features
- Full validation framework
- Error handling with database logging
- Three-layer data architecture
- Star schema modeling
- Sample analytical queries

**📝Future Enhancement to implement:**
- Parallel processing (code comments show approach)
- Advanced monitoring (integration points defined)
- Kubernetes deployment (example manifests)
- Comprehensive test suite (structure defined)

