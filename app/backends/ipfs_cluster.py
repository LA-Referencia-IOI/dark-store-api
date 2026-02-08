"""
IPFS Cluster storage backend.

Uses IPFS API for content operations and IPFS Cluster REST for pin management.
"""

import logging
from typing import Any

import httpx

from .base import (
    StorageBackend,
    ContentInfo,
    PinStatus,
    StorageError,
    ContentNotFoundError,
)


logger = logging.getLogger(__name__)


class IPFSClusterBackend(StorageBackend):
    """
    IPFS Cluster implementation of storage backend.

    Uses:
    - IPFS API (localhost:5001) for add/cat operations
    - IPFS Cluster REST API (localhost:9094) for pin status
    """

    def __init__(
        self,
        ipfs_api_url: str = "http://localhost:5001",
        cluster_api_url: str = "http://localhost:9094",
        timeout: float = 30.0,
    ):
        """
        Initialize IPFS Cluster backend.

        Args:
            ipfs_api_url: IPFS node API URL (e.g., http://localhost:5001)
            cluster_api_url: IPFS Cluster REST API URL (e.g., http://localhost:9094)
            timeout: HTTP request timeout in seconds
        """
        self.ipfs_api_url = ipfs_api_url.rstrip("/")
        self.cluster_api_url = cluster_api_url.rstrip("/")
        self.timeout = timeout

    async def store(self, content: bytes, content_type: str) -> ContentInfo:
        """
        Store content in IPFS and return CID.

        Uses IPFS add API with CIDv1.
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Add content to IPFS
                # Using wrap-with-directory=false and cid-version=1
                response = await client.post(
                    f"{self.ipfs_api_url}/api/v0/add",
                    params={"cid-version": 1, "pin": True},
                    files={"file": ("content", content, content_type)},
                )
                response.raise_for_status()

                result = response.json()
                cid = result["Hash"]
                size = int(result.get("Size", len(content)))

                logger.info(f"Stored content in IPFS: CID={cid}, size={size}")

                return ContentInfo(cid=cid, size=size, content_type=content_type)

        except httpx.HTTPStatusError as e:
            logger.error(f"IPFS API error: {e.response.status_code} - {e.response.text}")
            raise StorageError(f"IPFS storage failed: {e}")
        except httpx.RequestError as e:
            logger.error(f"IPFS connection error: {e}")
            raise StorageError(f"IPFS connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error storing content: {e}")
            raise StorageError(f"Storage failed: {e}")

    async def retrieve(self, cid: str) -> tuple[bytes, str]:
        """
        Retrieve content from IPFS by CID.

        Uses IPFS cat API.
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.ipfs_api_url}/api/v0/cat",
                    params={"arg": cid},
                )

                if response.status_code == 500:
                    # IPFS returns 500 for "not found" in some cases
                    error_text = response.text.lower()
                    if "not found" in error_text or "no link" in error_text:
                        raise ContentNotFoundError(f"Content not found for CID: {cid}")

                response.raise_for_status()

                content = response.content
                # IPFS doesn't preserve content-type, return as octet-stream
                content_type = "application/octet-stream"

                logger.debug(f"Retrieved content for CID: {cid}, size={len(content)}")
                return content, content_type

        except ContentNotFoundError:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"IPFS API error: {e.response.status_code}")
            raise StorageError(f"IPFS retrieval failed: {e}")
        except httpx.RequestError as e:
            logger.error(f"IPFS connection error: {e}")
            raise StorageError(f"IPFS connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error retrieving content: {e}")
            raise StorageError(f"Retrieval failed: {e}")

    async def status(self, cid: str) -> PinStatus:
        """
        Get pin status from IPFS Cluster.

        Uses Cluster REST API /pins/{cid} endpoint.
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.cluster_api_url}/pins/{cid}",
                )

                if response.status_code == 404:
                    raise ContentNotFoundError(f"Pin not found for CID: {cid}")

                response.raise_for_status()

                result = response.json()
                return self._parse_cluster_pin_status(cid, result)

        except ContentNotFoundError:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"Cluster API error: {e.response.status_code}")
            raise StorageError(f"Cluster status check failed: {e}")
        except httpx.RequestError as e:
            logger.error(f"Cluster connection error: {e}")
            raise StorageError(f"Cluster connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error checking status: {e}")
            raise StorageError(f"Status check failed: {e}")

    def _parse_cluster_pin_status(self, cid: str, result: dict[str, Any]) -> PinStatus:
        """Parse IPFS Cluster pin status response."""
        peer_map = result.get("peer_map", {})

        # Count replicas by status
        pinned_count = 0
        statuses = []

        for peer_id, peer_status in peer_map.items():
            status = peer_status.get("status", "unknown")
            statuses.append(status)
            if status == "pinned":
                pinned_count += 1

        # Determine overall status
        if pinned_count > 0:
            overall_status = "pinned"
        elif "pinning" in statuses:
            overall_status = "pinning"
        elif "error" in statuses:
            overall_status = "error"
        else:
            overall_status = "unpinned"

        return PinStatus(
            cid=cid,
            pinned=pinned_count > 0,
            replicas=pinned_count,
            status=overall_status,
        )

    async def health_check(self) -> bool:
        """Check if IPFS node and Cluster are accessible."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Check IPFS node
                ipfs_response = await client.post(f"{self.ipfs_api_url}/api/v0/id")
                if ipfs_response.status_code != 200:
                    logger.error(f"IPFS node unhealthy: {ipfs_response.status_code}")
                    return False

                # Check Cluster
                cluster_response = await client.get(f"{self.cluster_api_url}/id")
                if cluster_response.status_code != 200:
                    logger.error(f"Cluster unhealthy: {cluster_response.status_code}")
                    return False

                return True

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
