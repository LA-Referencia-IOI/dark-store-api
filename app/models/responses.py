"""
Pydantic response models for the store API.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class ReplicationResponse(BaseModel):
    """Current replication distribution and purge readiness."""

    total_replicas: int
    queued_replicas: int = 0
    pinning_replicas: int = 0
    error_replicas: int = 0
    assigned_replicas: int = 0
    checked_at: datetime


class StoreResponse(BaseModel):
    """Response from store operation."""

    cid: str = Field(..., description="Content Identifier (CID)")
    size: int = Field(..., description="Size in bytes")
    replication: Optional[ReplicationResponse] = None


class StatusResponse(BaseModel):
    """Response from status check."""

    cid: str = Field(..., description="Content Identifier")
    status: str = Field(..., description="Pin status: pinned, pinning, queued, unpinned, error, unknown")
    replication: ReplicationResponse
    # Batch observation is deliberately partial: an unavailable CID must not
    # turn an otherwise useful page into a global 503.
    error: Optional[str] = None


class BatchStatusRequest(BaseModel):
    """Bounded CID list for a single replication observation round."""

    cids: list[str] = Field(..., min_length=1, max_length=200)


class BatchStatusResponse(BaseModel):
    statuses: list[StatusResponse]


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Service status: healthy, unhealthy")
    backend: str = Field(..., description="Storage backend type")
    backend_healthy: bool = Field(..., description="Backend health status")
    min_cluster_peers: Optional[int] = Field(default=None, description="Minimum required IPFS Cluster peers")
    available_cluster_peers: Optional[int] = Field(default=None, description="Currently visible IPFS Cluster peers")
    duplicate_ipfs_peer_ids: Optional[dict[str, list[str]]] = Field(default=None, description="Invalid duplicate Kubo identities observed behind Cluster peers")
    dimension: Optional[str] = Field(default=None, description="Health dimension: read or write")
    error: Optional[str] = Field(default=None, description="Backend health error detail when unhealthy")
    cached: bool = Field(default=False, description="Whether this response used cached backend readiness")
    checked_at: Optional[datetime] = Field(default=None, description="When backend readiness was last checked")
    cache_ttl_seconds: Optional[float] = Field(default=None, description="Backend readiness cache TTL")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ErrorResponse(BaseModel):
    """Error response."""

    error: str = Field(..., description="Error type")
    detail: str = Field(..., description="Error details")
