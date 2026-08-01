"""
Pydantic response models for the store API.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class ReplicationResponse(BaseModel):
    """Durability evidence observed before a successful store response."""

    status: str
    pinned_peers: int
    pinned_sites: int
    target_peers: int


class StoreResponse(BaseModel):
    """Response from store operation."""

    cid: str = Field(..., description="Content Identifier (CID)")
    size: int = Field(..., description="Size in bytes")
    replication: Optional[ReplicationResponse] = None


class StatusResponse(BaseModel):
    """Response from status check."""

    cid: str = Field(..., description="Content Identifier")
    pinned: bool = Field(..., description="Whether content is pinned")
    replicas: int = Field(..., description="Number of replicas")
    status: str = Field(..., description="Pin status: pinned, pinning, unpinned, error")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Service status: healthy, unhealthy")
    backend: str = Field(..., description="Storage backend type")
    backend_healthy: bool = Field(..., description="Backend health status")
    min_cluster_peers: Optional[int] = Field(default=None, description="Minimum required IPFS Cluster peers")
    available_cluster_peers: Optional[int] = Field(default=None, description="Currently visible IPFS Cluster peers")
    min_cluster_sites: Optional[int] = Field(default=None, description="Minimum required sites for writes")
    available_cluster_sites: Optional[int] = Field(default=None, description="Currently visible Cluster sites")
    expected_cluster_peers: Optional[int] = Field(default=None, description="Peers defined by the global topology")
    dimension: Optional[str] = Field(default=None, description="Health dimension: read or write")
    error: Optional[str] = Field(default=None, description="Backend health error detail when unhealthy")
    cached: bool = Field(default=False, description="Whether this response used cached backend readiness")
    checked_at: Optional[datetime] = Field(default=None, description="When backend readiness was last checked")
    cache_ttl_seconds: Optional[float] = Field(default=None, description="Backend readiness cache TTL")
    add_mode: Optional[str] = Field(default=None, description="IPFS add mode used by the backend")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ErrorResponse(BaseModel):
    """Error response."""

    error: str = Field(..., description="Error type")
    detail: str = Field(..., description="Error details")
