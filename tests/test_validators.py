# ============================================================================
# FILE: tests/test_validators.py
# ============================================================================
import pytest
from earthquake_elt.ingestion import DataValidator


@pytest.fixture
def sample_config():
    return {
        "validation": {
            "required_fields": [
                "id",
                "properties.mag",
                "properties.time",
                "geometry.coordinates",
            ],
            "magnitude_range": [0.0, 10.0],
            "depth_range": [-10.0, 800.0],
        }
    }


@pytest.fixture
def valid_event():
    return {
        "id": "test123",
        "properties": {
            "mag": 5.2,
            "magType": "mw",
            "place": "Test Location",
            "time": 1699999999000,
        },
        "geometry": {"coordinates": [-122.4, 37.8, 10.5]},
    }


def test_validate_event_success(sample_config, valid_event):
    validator = DataValidator(sample_config)
    is_valid, error = validator.validate_event(valid_event)
    assert is_valid
    assert error is None


def test_validate_event_missing_field(sample_config):
    validator = DataValidator(sample_config)
    invalid_event = {"id": "test"}
    is_valid, error = validator.validate_event(invalid_event)
    assert not is_valid
    assert error is not None


def test_validate_batch(sample_config, valid_event):
    validator = DataValidator(sample_config)
    valid, invalid = validator.validate_batch([valid_event])
    assert len(valid) == 1
    assert len(invalid) == 0
