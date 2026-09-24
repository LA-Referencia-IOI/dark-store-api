"""Configuration coverage for dark-store-api."""

import pytest
from pydantic import ValidationError

from app.config import Settings


def test_log_level_defaults_to_warning() -> None:
    assert Settings(_env_file=None).log_level == "WARNING"


@pytest.mark.parametrize("value", ["debug", "INFO", "Warning", "ERROR", "critical"])
def test_log_level_accepts_supported_values(value: str) -> None:
    assert Settings(_env_file=None, log_level=value).log_level == value.upper()


def test_log_level_rejects_unsupported_value() -> None:
    with pytest.raises(ValidationError, match="LOG_LEVEL must be"):
        Settings(_env_file=None, log_level="verbose")
