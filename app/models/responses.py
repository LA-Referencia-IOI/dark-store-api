"""
Pydantic response models for the store API.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class StoreResponse(BaseModel):
    """Response from store operation."""

    cid: str = Field(..., description="Content Identifier (CID)")
    size: int = Field(..., description="Size in bytes")


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
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ErrorResponse(BaseModel):
    """Error response."""

    error: str = Field(..., description="Error type")
    detail: str = Field(..., description="Error details")
