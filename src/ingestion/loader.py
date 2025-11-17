# ============================================================================
# src/ingestion/loader.py
# ============================================================================

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class RawDataLoader:
    """
    Load validated data into raw layer with metadata tracking.
    """

    def __init__(self, database):
        self.db = database

    def load_batch(
            self,
            events: List[Dict[str, Any]],
            batch_id: str = None
    ) -> Dict[str, int]:
        """
        Load batch of events to raw layer.

        Returns:
            Statistics dictionary with counts
        """

        if not batch_id:
            batch_id = str(uuid.uuid4())

        #start_time = datetime.utcnow()
        start_time = datetime.now(timezone.utc)

        try:
            # Prepare records for insertion
            records = []
            for event in events:
                records.append({
                    'batch_id': batch_id,
                    'event_id': event['id'],
                    'raw_data': json.dumps(event),
                    'ingested_at': datetime.now(timezone.utc)
                })

            # Bulk insert
            inserted = self.db.bulk_insert('raw_earthquake_events', records)

            # Log batch metadata
            self._log_batch_metadata(
                batch_id=batch_id,
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                records_fetched=len(events),
                records_inserted=inserted,
                status='success'
            )

            logger.info(f"Loaded {inserted} events to raw layer (batch: {batch_id})")

            return {
                'batch_id': batch_id,
                'inserted': inserted,
                'failed': 0
            }

        except Exception as e:
            logger.error(f"Failed to load batch: {str(e)}")

            self._log_batch_metadata(
                batch_id=batch_id,
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                records_fetched=len(events),
                records_inserted=0,
                status='failed',
                error_message=str(e)
            )

            raise

    def _log_batch_metadata(
            self,
            batch_id: str,
            start_time: datetime,
            end_time: datetime,
            records_fetched: int,
            records_inserted: int,
            status: str,
            error_message: str = None
    ) -> None:
        """Log batch ingestion metadata."""

        metadata = {
            'batch_id': batch_id,
            'start_time': start_time,
            'end_time': end_time,
            'records_fetched': records_fetched,
            'records_inserted': records_inserted,
            'status': status,
            'error_message': error_message
        }

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ingestion_metadata 
                    (batch_id, start_time, end_time, records_fetched, 
                     records_inserted, status, error_message)
                    VALUES (%(batch_id)s, %(start_time)s, %(end_time)s,
                            %(records_fetched)s, %(records_inserted)s,
                            %(status)s, %(error_message)s)
                """, metadata)