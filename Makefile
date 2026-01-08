# ============================================================================
# FILE: Makefile
# ============================================================================
.PHONY: help setup run test lint build airflow-build airflow-init airflow-up airflow-down clean

help:
	@echo "Available commands:"
	@echo "  make setup            - Setup local dev environment"
	@echo "  make run              - Run pipeline locally (no Docker)"
	@echo "  make test             - Run tests"
	@echo "  make lint             - Run ruff & black"
	@echo "  make airflow-build    - Build Airflow Docker image"
	@echo "  make airflow-init     - Initialize Airflow metadata DB"
	@echo "  make airflow-up       - Start Airflow + Postgres"
	@echo "  make airflow-down     - Stop Airflow stack"
	@echo "  make clean            - Cleanup containers & volumes"

# -----------------------------
# Local Python (NO Airflow)
# -----------------------------
setup:
	@echo "Setting up virtual environment..."
	pip install -r requirements.txt
	pip install -e .

run:
	@echo "Running pipeline locally (no Docker, no Airflow)..."
	python run_pipeline.py

test:
	pytest -v

lint:
	ruff check .
	black --check .

# -----------------------------
# Airflow / Docker
# -----------------------------
airflow-build:
	@echo "Building custom Airflow image..."
	docker build -f Dockerfile.airflow -t earthquake-airflow:latest .

airflow-init:
	@echo "Initializing Airflow metadata database..."
	docker compose up -d postgres
	sleep 5
	docker compose run --rm airflow-webserver airflow db init
	docker compose run --rm airflow-webserver airflow users create \
		--username admin \
		--password admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

airflow-up: airflow-build
	@echo "Starting Airflow stack..."
	docker compose up -d

query:
	docker compose exec -T postgres psql -U postgres -d earthquake_db < sql/analytics/sample_queries.sql

airflow-down:
	docker compose down

# -----------------------------
# Cleanup
# -----------------------------
clean:
	docker compose down -v
	docker system prune -f
