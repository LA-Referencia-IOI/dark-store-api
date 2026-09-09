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

    # Generated endpoint pool, mounted read-only by Compose. The deployer has
    # already resolved its access group; Store API does not know topology/site.
    storage_endpoints_file: Path = Path("/config/storage-endpoints.json")
    ipfs_health_cache_ttl_seconds: float = 10.0
    replication_target_replicas: int = 2
    store_add_concurrency: int = 4
    store_status_concurrency: int = 6
    store_promotion_concurrency: int = 4

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
