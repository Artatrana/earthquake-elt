# ============================================================================
# FILE: src/ingestion/api_client.py
# ============================================================================
import time
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for API requests."""

    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call = 0.0

    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_call = time.time()


class USGSAPIClient:
    """USGS Earthquake Catalog API client with production features."""

    def __init__(self, config: Dict[str, Any]):
        self.base_url = config["api"]["base_url"]
        self.format = config["api"]["format"]
        self.timeout = config["api"]["timeout"]
        self.batch_size = config["api"]["batch_size"]
        self.rate_limiter = RateLimiter(config["api"]["rate_limit_per_minute"])
        self.session = requests.Session()
        logger.info(f"Initialized USGS API client: {self.base_url}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
    )
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request with retry logic."""
        self.rate_limiter.wait_if_needed()
        logger.info(f"API request with params: {params}")
        try:
            response = self.session.get(
                self.base_url, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            logger.info(
                f"API response: {data.get('metadata', {}).get('count', 0)} events"
            )
            return data
        except requests.Timeout:
            logger.error(f"Request timeout after {self.timeout}s")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    def fetch_earthquakes(
        self,
        start_time: datetime,
        end_time: datetime,
        min_magnitude: Optional[float] = None,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch earthquake events with pagination support."""
        all_events = []
        offset = 1
        while True:
            params = {
                "format": self.format,
                "starttime": start_time.isoformat(),
                "endtime": end_time.isoformat(),
                "limit": self.batch_size,
                "offset": offset,
                "orderby": "time",
            }
            if min_magnitude:
                params["minmagnitude"] = min_magnitude
            try:
                data = self._make_request(params)
                features = data.get("features", [])
                if not features:
                    logger.info("No more events to fetch")
                    break
                all_events.extend(features)
                logger.info(
                    f"Fetched {len(features)} events (total: {len(all_events)})"
                )
                if max_results and len(all_events) >= max_results:
                    all_events = all_events[:max_results]
                    break
                metadata = data.get("metadata", {})
                total_count = metadata.get("count", 0)
                if len(all_events) >= total_count:
                    logger.info("Fetched all available events")
                    break
                offset += len(features)
            except Exception as e:
                logger.error(f"Failed to fetch batch at offset {offset}: {str(e)}")
                break
        logger.info(f"Total events fetched: {len(all_events)}")
        return all_events
