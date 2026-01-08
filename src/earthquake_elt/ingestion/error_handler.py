# ============================================================================
# FILE: src/ingestion/error_handler.py
# ============================================================================
from typing import Dict, Any
from datetime import datetime, timezone
import logging
import json

logger = logging.getLogger(__name__)


class ErrorHandler:
    """Centralized error handling with database persistence."""

    def __init__(self, database, config: Dict[str, Any]):
        self.db = database
        self.max_errors = config["ingestion"]["max_errors_per_batch"]
        self.error_count = 0

    def log_error(
        self,
        event_id: str,
        error_type: str,
        error_message: str,
        raw_data: Dict[str, Any],
        batch_id: str,
    ) -> None:
        """Log error to database."""
        error_record = {
            "batch_id": batch_id,
            "event_id": event_id,
            "error_type": error_type,
            "error_message": error_message,
            "raw_data": json.dumps(raw_data),
            "occurred_at": datetime.now(timezone.utc),
        }
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ingestion_errors 
                        (batch_id, event_id, error_type, error_message, raw_data
                         , occurred_at)
                        VALUES (%(batch_id)s, %(event_id)s, %(error_type)s, 
                                %(error_message)s, %(raw_data)s, %(occurred_at)s)
                    """,
                        error_record,
                    )
            self.error_count += 1
            logger.error(f"Logged error for event {event_id}: {error_message}")
        except Exception as e:
            logger.error(f"Failed to log error: {str(e)}")

    def check_threshold(self) -> bool:
        """Check if error threshold exceeded."""
        if self.error_count >= self.max_errors:
            logger.critical(
                f"Error threshold exceeded: {self.error_count}/{self.max_errors}"
            )
            return True
        return False

    def reset(self):
        """Reset error counter."""
        self.error_count = 0
