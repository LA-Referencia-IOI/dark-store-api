"""
FastAPI dependency injection for storage backend.
"""

from functools import lru_cache

from .backends import StorageBackend, FileSystemBackend
from .backends.ipfs_cluster import IPFSClusterBackend
from .config import get_settings


@lru_cache
def get_storage_backend() -> StorageBackend:
    """
    Get the configured storage backend instance.

    Returns cached singleton based on STORAGE_BACKEND setting.
    """
    settings = get_settings()

    if settings.storage_backend == "filesystem":
        return FileSystemBackend(settings.filesystem_storage_path)
    elif settings.storage_backend == "ipfs_cluster":
        return IPFSClusterBackend(
            ipfs_api_url=settings.ipfs_api_url,
            cluster_api_url=settings.ipfs_cluster_api_url,
            cluster_proxy_api_url=settings.ipfs_cluster_proxy_api_url,
            add_mode=settings.ipfs_add_mode,
            health_cache_ttl_seconds=settings.ipfs_health_cache_ttl_seconds,
            min_cluster_peers=settings.ipfs_cluster_min_peers,
        )
    else:
        raise ValueError(f"Unknown storage backend: {settings.storage_backend}")
