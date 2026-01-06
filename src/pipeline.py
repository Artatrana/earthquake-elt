# ============================================================================
# FILE: src/pipeline.py
# ============================================================================
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
import uuid

from src.config import load_config
from src.database import Database
from src.ingestion.api_client import USGSAPIClient
from src.ingestion.validators import DataValidator
from src.ingestion.error_handler import ErrorHandler
from src.ingestion.loader import RawDataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class EarthquakePipeline:
    """Main ELT pipeline orchestrator."""

    def __init__(self, config_path: str = "config/config.toml"):
        self.config = load_config(config_path)
        self.db = Database(self.config)
        self.api_client = USGSAPIClient(self.config)
        self.validator = DataValidator(self.config)
        self.loader = RawDataLoader(self.db)
        self.error_handler = ErrorHandler(self.db, self.config)
        logger.info("Pipeline initialized")

    def run_ingestion(
        self,
        start_time: datetime = None,
        end_time: datetime = None,
        lookback_days: int = None,
    ) -> Dict[str, Any]:
        """Run ingestion phase."""
        batch_id = str(uuid.uuid4())
        logger.info(f"Starting ingestion (batch: {batch_id})")
        try:
            if not end_time:
                # end_time = datetime.utcnow()
                end_time = datetime.now(timezone.utc)
            if not start_time:
                lookback = lookback_days or self.config["api"]["lookback_days"]
                start_time = end_time - timedelta(days=lookback)
            logger.info(f"Fetching events from {start_time} to {end_time}")

            # Extract
            events = self.api_client.fetch_earthquakes(start_time, end_time)
            logger.info(f"Fetched {len(events)} events from API")
            if not events:
                logger.warning("No events returned")
                return {"status": "success", "events_fetched": 0}

            # Validate
            valid_events, invalid_events = self.validator.validate_batch(events)
            logger.info(
                f"Validation: {len(valid_events)} valid, {len(invalid_events)} invalid"
            )

            # Log errors
            for event, error_msg in invalid_events:
                self.error_handler.log_error(
                    event_id=event.get("id", "unknown"),
                    error_type="validation_error",
                    error_message=error_msg,
                    raw_data=event,
                    batch_id=batch_id,
                )

            if self.error_handler.check_threshold():
                raise Exception("Error threshold exceeded")

            # Load
            if valid_events:
                load_stats = self.loader.load_batch(valid_events, batch_id)
                logger.info(f"Loaded {load_stats['inserted']} events to raw layer")

            return {
                "status": "success",
                "batch_id": batch_id,
                "events_fetched": len(events),
                "events_valid": len(valid_events),
                "events_invalid": len(invalid_events),
                "events_loaded": len(valid_events),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }
        except Exception as e:
            logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
            raise

    def run_transformations(self) -> Dict[str, Any]:
        """Run SQL transformations."""
        logger.info("Starting transformations")
        try:
            logger.info("Transforming raw → staging")
            self.db.execute_sql_file("sql/transformations/load_staging.sql")
            logger.info("Transforming staging → warehouse")
            self.db.execute_sql_file("sql/transformations/load_warehouse.sql")
            stats = self._get_layer_counts()
            logger.info(f"Transformations complete: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Transformations failed: {str(e)}", exc_info=True)
            raise

    def run_full_pipeline(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> Dict[str, Any]:
        """Run complete pipeline."""
        logger.info("=" * 80)
        logger.info("Starting full pipeline execution")
        logger.info("=" * 80)
        # pipeline_start = datetime.utcnow()
        pipeline_start = datetime.now(timezone.utc)
        try:
            ingestion_stats = self.run_ingestion(start_time, end_time)
            transform_stats = self.run_transformations()
            # pipeline_end = datetime.utcnow()
            pipeline_end = datetime.now(timezone.utc)
            duration = (pipeline_end - pipeline_start).total_seconds()
            stats = {
                "status": "success",
                "duration_seconds": duration,
                "ingestion": ingestion_stats,
                "transformations": transform_stats,
                "completed_at": pipeline_end.isoformat(),
            }
            logger.info("=" * 80)
            logger.info(f"Pipeline completed in {duration:.2f}s")
            logger.info("=" * 80)
            return stats
        except Exception:
            logger.error("Pipeline failed", exc_info=True)
            raise
        finally:
            self.db.close_pool()

    def _get_layer_counts(self) -> Dict[str, int]:
        """Get record counts from each layer."""
        counts = {}
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_earthquake_events")
                counts["raw_events"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM stg_earthquakes")
                counts["staging_events"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM fact_earthquake_events")
                counts["fact_events"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM dim_time")
                counts["dim_time"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM dim_location")
                counts["dim_location"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM dim_event_type")
                counts["dim_event_type"] = cur.fetchone()[0]
        return counts
