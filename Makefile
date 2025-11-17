# ============================================================================
# FILE: Makefile
# ============================================================================
.PHONY: help setup run ingest transform test query clean

help:
	@echo "Earthquake ELT Pipeline Commands:"
	@echo "  make setup      - Setup environment"
	@echo "  make run        - Run full pipeline"
	@echo "  make ingest     - Run ingestion only"
	@echo "  make transform  - Run transformations only"
	@echo "  make test       - Run tests"
	@echo "  make query      - Run sample queries"
	@echo "  make clean      - Clean up"

setup:
	@echo "Setting up environment..."
	pip install -r requirements.txt
	mkdir -p logs
	cp .env.example .env
	docker-compose up -d
	@echo "Waiting for database..."
	sleep 10
	@echo "Initializing schema..."
	docker-compose exec -T postgres psql -U postgres -d earthquake_db < sql/schema/01_raw_layer.sql
	docker-compose exec -T postgres psql -U postgres -d earthquake_db < sql/schema/02_staging_layer.sql
	docker-compose exec -T postgres psql -U postgres -d earthquake_db < sql/schema/03_warehouse_layer.sql
	@echo "✓ Setup complete!"

run:
	python run_pipeline.py

ingest:
	python run_pipeline.py --ingestion-only

transform:
	python run_pipeline.py --transform-only

test:
	pytest tests/ -v

query:
	docker-compose exec -T postgres psql -U postgres -d earthquake_db < sql/analytics/sample_queries.sql

clean:
	docker-compose down -v
	rm -rf logs/*.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
