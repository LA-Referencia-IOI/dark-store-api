"""
FastAPI dependency injection for storage backend.
"""

from functools import lru_cache

from .backends import StorageBackend, FileSystemBackend
from .backends.ipfs_cluster import IPFSClusterBackend
from .config import get_settings
from .topology import load_endpoints


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
        endpoints = load_endpoints(settings.storage_endpoints_file)
        return IPFSClusterBackend(
            ipfs_api_urls=endpoints["ipfs"],
            cluster_api_urls=endpoints["cluster"],
            health_cache_ttl_seconds=settings.ipfs_health_cache_ttl_seconds,
        )
    else:
        raise ValueError(f"Unknown storage backend: {settings.storage_backend}")
