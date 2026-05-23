"""
Configuration settings for dark-store-api.

Uses Pydantic Settings for environment variable management.
"""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_env_file() -> str:
    """Prefer local integration settings when present."""
    project_root = Path(__file__).resolve().parents[1]
    integration_env = project_root / ".env.integration"
    default_env = project_root / ".env"

    if integration_env.exists():
        return str(integration_env)

    return str(default_env)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Backend selection: "ipfs_cluster" or "filesystem"
    storage_backend: str = "ipfs_cluster"

    # IPFS endpoints (from dark-ipfs cluster)
    ipfs_api_url: str = "http://localhost:5001"
    ipfs_cluster_api_url: str = "http://localhost:9094"
    ipfs_cluster_proxy_api_url: str = "http://localhost:9095"
    ipfs_add_mode: str = "cluster_proxy"
    ipfs_health_cache_ttl_seconds: float = 10.0
    ipfs_cluster_min_peers: int = 2

    # Filesystem backend (for dev/test only)
    filesystem_storage_path: str = "./storage"

    # API server settings (STORE_API_* or fallback to API_*)
    store_api_host: str = "0.0.0.0"
    store_api_port: int = 8003

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
    return Settings(_env_file=_resolve_env_file())
