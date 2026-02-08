"""
Storage API endpoints.

Provides store, retrieve, status, and health operations.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request, Response, HTTPException

from ..backends.base import StorageBackend, ContentNotFoundError, StorageError
from ..config import get_settings
from ..dependencies import get_storage_backend
from ..models.responses import StoreResponse, StatusResponse, HealthResponse, ErrorResponse


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["storage"])
health_router = APIRouter(tags=["health"])


@router.post(
    "/store",
    response_model=StoreResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Store content",
    description="Store raw content and return its CID. Content-Type header determines format.",
)
async def store_content(
    request: Request,
    backend: StorageBackend = Depends(get_storage_backend),
) -> StoreResponse:
    """
    Store content and return CID.

    The Content-Type header is preserved for retrieval.
    Accepts any content type: application/json, text/xml, text/plain, etc.
    """
    content_type = request.headers.get("content-type", "application/octet-stream")
    # Strip charset and other parameters for storage
    content_type = content_type.split(";")[0].strip()

    body = await request.body()

    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    try:
        info = await backend.store(body, content_type)

        logger.info(f"Stored content: cid={info.cid}, size={info.size}, type={content_type}")

        return StoreResponse(
            cid=info.cid,
            size=info.size,
            content_type=info.content_type,
        )

    except StorageError as e:
        logger.error(f"Storage error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/retrieve/{cid}",
    responses={
        200: {"description": "Raw content with original Content-Type"},
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

    Returns raw content with the original Content-Type header.
    """
    try:
        content, content_type = await backend.retrieve(cid)

        logger.debug(f"Retrieved content: cid={cid}, size={len(content)}")

        return Response(
            content=content,
            media_type=content_type,
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
            pinned=status.pinned,
            replicas=status.replicas,
            status=status.status,
        )

    except ContentNotFoundError:
        raise HTTPException(status_code=404, detail=f"Pin not found: {cid}")
    except StorageError as e:
        logger.error(f"Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@health_router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check API and backend health.",
)
async def health_check(
    backend: StorageBackend = Depends(get_storage_backend),
) -> HealthResponse:
    """
    Check API and storage backend health.
    """
    settings = get_settings()
    backend_healthy = await backend.health_check()

    return HealthResponse(
        status="healthy" if backend_healthy else "unhealthy",
        backend=settings.storage_backend,
        backend_healthy=backend_healthy,
        timestamp=datetime.now(timezone.utc),
    )
