"""Entrypoint coverage for dark-store-api."""

from app.config import get_settings
from app import main


def test_run_passes_configured_log_level_to_uvicorn(monkeypatch) -> None:
    captured = {}

    def fake_run(*args, **kwargs) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setenv("LOG_LEVEL", "warning")
    monkeypatch.setattr(main.uvicorn, "run", fake_run)
    get_settings.cache_clear()
    try:
        main.run()
    finally:
        get_settings.cache_clear()

    assert captured["args"] == ("app.main:app",)
    assert captured["kwargs"]["log_level"] == "warning"
