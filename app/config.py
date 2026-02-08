"""
Configuration settings for dark-store-api.

Uses Pydantic Settings for environment variable management.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Backend selection: "ipfs_cluster" or "filesystem"
    storage_backend: str = "ipfs_cluster"

    # IPFS endpoints (from dark-ipfs cluster)
    ipfs_api_url: str = "http://localhost:5001"
    ipfs_cluster_api_url: str = "http://localhost:9094"

    # Filesystem backend (for dev/test only)
    filesystem_storage_path: str = "./storage"

    # API server settings (STORE_API_* or fallback to API_*)
    store_api_host: str = "0.0.0.0"
    store_api_port: int = 8002

    # Logging
    log_level: str = "INFO"

    @property
    def api_host(self) -> str:
        """Get API host (for compatibility)."""
        return self.store_api_host

    @property
    def api_port(self) -> int:
        """Get API port (for compatibility)."""
        return self.store_api_port


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
