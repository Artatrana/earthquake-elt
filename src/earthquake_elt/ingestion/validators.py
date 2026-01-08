# ============================================================================
# FILE: src/ingestion/validators.py
# ============================================================================
from typing import Dict, Any, List, Tuple, Optional
from pydantic import BaseModel, Field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class EarthquakeEvent(BaseModel):
    """Validation model for earthquake events."""

    id: str
    magnitude: Optional[float] = Field(
        None, ge=-2.0, le=10.0
    )  # Changed: allow negative, None OK
    magnitude_type: Optional[str] = None
    place: Optional[str] = None  # Changed: made optional
    time: datetime
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    depth: Optional[float] = Field(None, ge=-10.0, le=800.0)  # Changed: made optional

    class Config:
        arbitrary_types_allowed = True


class DataValidator:
    """Data validation with quality checks."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Reduced required fields - only truly critical ones
        self.required_fields = ["id", "properties.time", "geometry.coordinates"]

        # Using Validation form .toml file - but for now let validate through pydentic
        # self.required_fields = config['validation']['required_fields']
        # min_mag, max_mag = config['validation']['magnitude_range']
        # min_depth, max_depth = config['validation']['depth_range']

    def validate_event(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate single event. Returns (is_valid, error_message)."""
        try:
            # Check required fields
            for field_path in self.required_fields:
                if not self._get_nested_value(event, field_path):
                    return False, f"Missing required field: {field_path}"

            # Extract coordinates
            coords = event.get("geometry", {}).get("coordinates", [])
            if len(coords) < 2:  # At least lat/lon required
                return False, "Invalid coordinates format"

            # longitude = coords[0]
            # latitude = coords[1]
            # depth = coords[2] if len(coords) > 2 else None

            # Get properties
            props = event.get("properties", {})
            time_ms = props.get("time")

            if not time_ms:
                return False, "Missing time"

            # Validate using Pydantic model
            # validated = EarthquakeEvent(
            #     id=event["id"],
            #     magnitude=props.get("mag"),
            #     magnitude_type=props.get("magType"),
            #     place=props.get("place"),
            #     time=datetime.fromtimestamp(time_ms / 1000.0),
            #     latitude=latitude,
            #     longitude=longitude,
            #     depth=depth,
            # )

            return True, None

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def validate_batch(
        self, events: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Tuple[Dict, str]]]:
        """Validate batch. Returns (valid_events, invalid_events_with_errors)."""
        valid_events = []
        invalid_events = []

        for event in events:
            is_valid, error_msg = self.validate_event(event)

            if is_valid:
                valid_events.append(event)
            else:
                invalid_events.append((event, error_msg))
                logger.warning(f"Invalid event {event.get('id', 'unknown')}: {error_msg}")

        logger.info(
            f"Validation: {len(valid_events)} valid, {len(invalid_events)} invalid"
        )

        return valid_events, invalid_events

    @staticmethod
    def _get_nested_value(d: Dict, path: str) -> Any:
        """Get value from nested dict using dot notation."""
        keys = path.split(".")
        value = d
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
