"""
Test fixtures for dark-store-api.
"""

import os
import tempfile
from typing import Generator

import pytest
from fastapi.testclient import TestClient

# Set to filesystem backend for tests
os.environ["STORAGE_BACKEND"] = "filesystem"


@pytest.fixture
def temp_storage() -> Generator[str, None, None]:
    """Create a temporary storage directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["FILESYSTEM_STORAGE_PATH"] = tmpdir
        yield tmpdir


@pytest.fixture
def client(temp_storage: str) -> TestClient:
    """Create a test client with filesystem backend."""
    # Clear cached dependencies
    from app.dependencies import get_storage_backend
    from app.config import get_settings
    
    get_storage_backend.cache_clear()
    get_settings.cache_clear()
    
    from app.main import app
    
    return TestClient(app)
