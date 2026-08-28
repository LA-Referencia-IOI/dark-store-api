"""
Configuration settings for dark-store-api.

Uses Pydantic Settings for environment variable management.
"""

from functools import lru_cache
from pathlib import Path
from pydantic import Field
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

    # Both site-local storage nodes. Values are JSON arrays in the environment.
    ipfs_api_urls_json: list[str] = Field(default_factory=list)
    ipfs_cluster_api_urls_json: list[str] = Field(default_factory=list)
    ipfs_cluster_proxy_api_urls_json: list[str] = Field(default_factory=list)
    # Maps Cluster peer names (the topology node IDs) to site IDs.
    ipfs_cluster_peer_sites_json: dict[str, str] = Field(default_factory=dict)
    ipfs_cluster_local_site_id: str = ""
    ipfs_health_cache_ttl_seconds: float = 10.0
    ipfs_replication_confirm_timeout_seconds: float = 120.0

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
