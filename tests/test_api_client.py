# ============================================================================
# FILE: tests/test_api_client.py
# ============================================================================
import pytest
from unittest.mock import Mock, patch
from src.ingestion.api_client import USGSAPIClient

@pytest.fixture
def sample_config():
    return {
        'api': {
            'base_url': 'https://earthquake.usgs.gov/fdsnws/event/1/query',
            'format': 'geojson',
            'timeout': 30,
            'batch_size': 1000,
            'rate_limit_per_minute': 60
        }
    }

@pytest.fixture
def sample_response():
    return {
        'type': 'FeatureCollection',
        'metadata': {'count': 1},
        'features': [{
            'type': 'Feature',
            'id': 'test123',
            'properties': {
                'mag': 5.2,
                'place': 'Test Location',
                'time': 1699999999000
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [-122.4, 37.8, 10.5]
            }
        }]
    }

def test_client_initialization(sample_config):
    client = USGSAPIClient(sample_config)
    assert client.base_url == sample_config['api']['base_url']
    assert client.timeout == sample_config['api']['timeout']

@patch('requests.Session.get')
def test_fetch_earthquakes_success(mock_get, sample_config, sample_response):
    mock_get.return_value.json.return_value = sample_response
    mock_get.return_value.status_code = 200
    client = USGSAPIClient(sample_config)
    from datetime import datetime
    events = client.fetch_earthquakes(
        datetime(2024, 1, 1),
        datetime(2024, 1, 2)
    )
    assert len(events) == 1
    assert events[0]['id'] == 'test123'
