"""
Storage API endpoints.

Provides store, retrieve, status, and health operations.
"""

import logging
from datetime import datetime, timezone
from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, Request, Response, HTTPException

from ..backends.base import (
    StorageBackend,
    ContentNotFoundError,
    StorageError,
)
from ..config import get_settings
from ..dependencies import get_storage_backend
from ..models.responses import (
    BatchStatusRequest,
    BatchStatusResponse,
    ErrorResponse,
    HealthResponse,
    ReplicationResponse,
    StatusResponse,
    StoreResponse,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["storage"])
health_router = APIRouter(tags=["health"])


class ReplicationEnsureRequest(BaseModel):
    cids: list[str] = Field(..., min_length=1, max_length=200)
    target_replicas: int = Field(..., ge=1, le=200)
    assigned_replicas: dict[str, int] = Field(default_factory=dict)


@router.post(
    "/store",
    response_model=StoreResponse,
    responses={500: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    summary="Store content",
    description="Store raw content and return its CID.",
)
async def store_content(
    request: Request,
    backend: StorageBackend = Depends(get_storage_backend),
) -> StoreResponse:
    """
    Store content and return CID.

    Accepts any payload: JSON, XML, text, or binary bytes.
    """
    body = await request.body()

    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    try:
        info = await backend.store(body)

        logger.info(f"Stored raw content: cid={info.cid}, size={info.size}")

        return StoreResponse(
            cid=info.cid,
            size=info.size,
            replication=(
                ReplicationResponse(**vars(info.replication))
                if info.replication is not None
                else None
            ),
        )

    except StorageError as e:
        logger.error(f"Storage error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/replication/ensure", summary="Request durable replication")
async def ensure_replication(
    request: ReplicationEnsureRequest,
    backend: StorageBackend = Depends(get_storage_backend),
) -> dict[str, dict[str, str]]:
    """Request higher allocations for existing CIDs without waiting for pins."""
    try:
        configured_target = get_settings().replication_target_replicas
        if request.target_replicas > configured_target:
            raise HTTPException(
                status_code=422,
                detail=f"target_replicas cannot exceed configured target {configured_target}",
            )
        already_allocated = {
            cid: "already_allocated"
            for cid in request.cids
            if int(request.assigned_replicas.get(cid, -1)) >= request.target_replicas
        }
        pending = [cid for cid in request.cids if cid not in already_allocated]
        promoted = await backend.ensure_replication(pending, request.target_replicas) if pending else {}
        return {"results": {**already_allocated, **promoted}}
    except StorageError as exc:
        logger.error("Replication promotion failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get(
    "/retrieve/{cid}",
    responses={
        200: {"description": "Raw content bytes"},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Retrieve content by CID",
    description="Retrieve previously stored content by its CID.",
)
async def retrieve_content(
    cid: str,
    backend: StorageBackend = Depends(get_storage_backend),
) -> Response:
    """
    Retrieve content by CID.

    Returns raw content bytes.
    """
    try:
        content = await backend.retrieve(cid)

        logger.debug(f"Retrieved content: cid={cid}, size={len(content)}")

        return Response(
            content=content,
            media_type="application/octet-stream",
        )

    except ContentNotFoundError:
        raise HTTPException(status_code=404, detail=f"Content not found: {cid}")
    except StorageError as e:
        logger.error(f"Retrieval error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/status/{cid}",
    response_model=StatusResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Get pin status",
    description="Get replication/pin status for a CID.",
)
async def get_status(
    cid: str,
    backend: StorageBackend = Depends(get_storage_backend),
) -> StatusResponse:
    """
    Get pin/replication status for a CID.

    Returns number of replicas and overall pin status.
    """
    try:
        status = await backend.status(cid)

        return StatusResponse(
            cid=status.cid,
            status=status.status,
            replication=ReplicationResponse(**vars(status.replication)),
        )

    except ContentNotFoundError:
        raise HTTPException(status_code=404, detail=f"Pin not found: {cid}")
    except StorageError as e:
        logger.error(f"Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/status/batch",
    response_model=BatchStatusResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Get pin status for a bounded CID batch",
)
async def get_status_batch(
    payload: BatchStatusRequest,
    backend: StorageBackend = Depends(get_storage_backend),
) -> BatchStatusResponse:
    """Observe CIDs concurrently without making callers open one request per CID."""
    import asyncio

    semaphore = asyncio.Semaphore(get_settings().store_status_concurrency)

    async def observe(cid: str) -> StatusResponse:
        try:
            async with semaphore:
                status = await backend.status(cid)
            return StatusResponse(
                cid=status.cid,
                status=status.status,
                replication=ReplicationResponse(**vars(status.replication)),
            )
        except ContentNotFoundError:
            return StatusResponse(
                cid=cid,
                status="unpinned",
                replication=ReplicationResponse(total_replicas=0, checked_at=datetime.now(timezone.utc)),
            )
        except StorageError as exc:
            logger.warning("CID status observation deferred cid=%s: %s", cid, exc)
            return StatusResponse(
                cid=cid,
                status="unknown",
                replication=ReplicationResponse(total_replicas=0, checked_at=datetime.now(timezone.utc)),
                error=str(exc),
            )

    statuses = await asyncio.gather(*(observe(cid) for cid in payload.cids))
    return BatchStatusResponse(statuses=statuses)


@health_router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check API and backend health.",
)
async def health_check(
    response: Response,
    refresh: bool = False,
    backend: StorageBackend = Depends(get_storage_backend),
) -> HealthResponse:
    """
    Check API and storage backend health.
    """
    settings = get_settings()
    write_check = getattr(backend, "write_health_check", None) or backend.health_check
    backend_healthy = await write_check(refresh=refresh)
    if not backend_healthy:
        response.status_code = 503
    backend_detail = getattr(backend, "last_health", {}) or {}

    return HealthResponse(
        status="healthy" if backend_healthy else "unhealthy",
        backend=settings.storage_backend,
        backend_healthy=backend_healthy,
        min_cluster_peers=backend_detail.get("min_cluster_peers"),
        available_cluster_peers=backend_detail.get("available_cluster_peers"),
        duplicate_ipfs_peer_ids=backend_detail.get("duplicate_ipfs_peer_ids"),
        dimension=backend_detail.get("dimension", "write"),
        error=backend_detail.get("error"),
        cached=bool(backend_detail.get("cached", False)),
        checked_at=backend_detail.get("checked_at"),
        cache_ttl_seconds=backend_detail.get("cache_ttl_seconds"),
        timestamp=datetime.now(timezone.utc),
    )


@health_router.get(
    "/health/read",
    response_model=HealthResponse,
    summary="Read readiness",
    description="Check that at least one site-local Kubo endpoint is usable.",
)
async def read_health_check(
    response: Response,
    backend: StorageBackend = Depends(get_storage_backend),
) -> HealthResponse:
    settings = get_settings()
    read_check = getattr(backend, "read_health_check", None) or backend.health_check
    backend_healthy = await read_check()
    if not backend_healthy:
        response.status_code = 503
    detail = getattr(backend, "last_health", {}) or {}
    return HealthResponse(
        status="healthy" if backend_healthy else "unhealthy",
        backend=settings.storage_backend,
        backend_healthy=backend_healthy,
        dimension=detail.get("dimension", "read"),
        error=detail.get("error"),
        timestamp=datetime.now(timezone.utc),
    )


@health_router.get(
    "/health/write",
    response_model=HealthResponse,
    summary="Write readiness",
    description="Check local endpoints and the configured global peer/site quorum.",
)
async def write_health_check(
    response: Response,
    refresh: bool = False,
    backend: StorageBackend = Depends(get_storage_backend),
) -> HealthResponse:
    return await health_check(response=response, refresh=refresh, backend=backend)


@health_router.get(
    "/health/live",
    summary="Liveness check",
    description="Check that the Store API process is alive without touching the storage backend.",
)
async def liveness_check() -> dict[str, str]:
    """Lightweight liveness check for Docker and load balancers."""
    return {"status": "alive"}
