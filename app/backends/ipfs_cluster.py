"""
IPFS Cluster storage backend.

Uses IPFS Cluster Proxy for writes, local IPFS API for reads, and IPFS
Cluster REST for readiness and pin status.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
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
    - IPFS Cluster Proxy (localhost:9095) for default add operations
    - IPFS API (localhost:5001) for cat operations and legacy add mode
    - IPFS Cluster REST API (localhost:9094) for readiness and pin status
    """

    def __init__(
        self,
        ipfs_api_url: str = "http://localhost:5001",
        cluster_api_url: str = "http://localhost:9094",
        cluster_proxy_api_url: str = "http://localhost:9095",
        add_mode: str = "cluster_proxy",
        timeout: float = 30.0,
        health_cache_ttl_seconds: float = 10.0,
        min_cluster_peers: int = 2,
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
        self.cluster_proxy_api_url = cluster_proxy_api_url.rstrip("/")
        self.add_mode = add_mode.strip().lower()
        self.timeout = timeout
        self.health_cache_ttl_seconds = max(float(health_cache_ttl_seconds), 0.0)
        self.min_cluster_peers = max(int(min_cluster_peers), 1)
        self.last_health: dict[str, Any] = {}
        self._last_health_result: bool | None = None

        if self.add_mode not in {"cluster_proxy", "ipfs_then_cluster"}:
            raise ValueError(
                "Unsupported IPFS add mode "
                f"'{add_mode}'. Expected 'cluster_proxy' or 'ipfs_then_cluster'."
            )

    async def store(self, content: bytes) -> ContentInfo:
        """
        Store content in IPFS and return CID.

        Uses IPFS Cluster Proxy by default so the resulting pin is managed by
        Cluster from the first write.
        """
        if self.add_mode == "ipfs_then_cluster":
            return await self._store_via_ipfs_then_cluster(content)
        return await self._store_via_cluster_proxy(content)

    async def _store_via_cluster_proxy(self, content: bytes) -> ContentInfo:
        """Store content through the Cluster Proxy IPFS API."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.cluster_proxy_api_url}/api/v0/add",
                    params={"cid-version": 1, "pin": True},
                    files={"file": ("content", content, "application/octet-stream")},
                )
                response.raise_for_status()

                info = self._parse_ipfs_add_response(response, fallback_size=len(content))
                logger.info(
                    "Stored raw content through IPFS Cluster Proxy: CID=%s, size=%s",
                    info.cid,
                    info.size,
                )
                return info

        except httpx.HTTPStatusError as e:
            logger.error(
                "Cluster Proxy API error: %s - %s",
                e.response.status_code,
                e.response.text,
            )
            raise StorageError(
                f"Cluster Proxy storage failed: HTTP {e.response.status_code} - {e.response.text}"
            )
        except httpx.RequestError as e:
            logger.error(f"Cluster Proxy connection error: {e}")
            raise StorageError(f"Cluster Proxy connection failed: {e}")
        except StorageError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error storing content through Cluster Proxy: {e}")
            raise StorageError(f"Storage failed: {e}")

    async def _store_via_ipfs_then_cluster(self, content: bytes) -> ContentInfo:
        """Legacy store path: add to local IPFS without pinning, then pin via Cluster."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.ipfs_api_url}/api/v0/add",
                    params={"cid-version": 1, "pin": False},
                    files={"file": ("content", content, "application/octet-stream")},
                )
                response.raise_for_status()

                info = self._parse_ipfs_add_response(response, fallback_size=len(content))
                cluster_response = await client.post(f"{self.cluster_api_url}/pins/{info.cid}")
                if cluster_response.status_code not in {200, 202, 409}:
                    raise StorageError(
                        "Cluster pin registration failed: "
                        f"HTTP {cluster_response.status_code} - {cluster_response.text}"
                    )

                logger.info("Stored raw content in IPFS then Cluster: CID=%s, size=%s", info.cid, info.size)
                return info

        except httpx.HTTPStatusError as e:
            logger.error(f"IPFS API error: {e.response.status_code} - {e.response.text}")
            raise StorageError(f"IPFS storage failed: {e}")
        except httpx.RequestError as e:
            logger.error(f"IPFS connection error: {e}")
            raise StorageError(f"IPFS connection failed: {e}")
        except StorageError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error storing content: {e}")
            raise StorageError(f"Storage failed: {e}")

    def _parse_ipfs_add_response(self, response: httpx.Response, fallback_size: int) -> ContentInfo:
        """Parse Kubo/Cluster Proxy add responses."""
        try:
            result = response.json()
        except ValueError as exc:
            raise StorageError(f"Invalid IPFS add response: {exc}") from exc

        if isinstance(result, list):
            if not result:
                raise StorageError("Invalid IPFS add response: empty list")
            result = result[-1]

        if not isinstance(result, dict):
            raise StorageError("Invalid IPFS add response: expected JSON object")

        cid = result.get("Hash")
        if not cid:
            raise StorageError("Invalid IPFS add response: missing Hash")

        try:
            size = int(result.get("Size", fallback_size))
        except (TypeError, ValueError):
            size = fallback_size

        return ContentInfo(cid=str(cid), size=size)

    async def retrieve(self, cid: str) -> bytes:
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
                logger.debug(f"Retrieved content for CID: {cid}, size={len(content)}")
                return content

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

    def _extract_cluster_peers(self, payload: Any) -> list[Any]:
        """Extract peer entries from common IPFS Cluster peers payload shapes."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            value = payload.get("peers")
            if isinstance(value, list):
                return value
            if payload.get("id"):
                return [payload]
            cluster_peers = payload.get("cluster_peers")
            if isinstance(cluster_peers, list):
                return [{"id": peer_id} for peer_id in cluster_peers if peer_id]
            return [payload]
        return []

    def _parse_cluster_peers_response(self, response: httpx.Response) -> list[Any]:
        """Parse /peers responses that may be JSON arrays or JSON streams."""
        try:
            return self._extract_cluster_peers(response.json())
        except ValueError:
            raw = getattr(response, "text", "") or ""
            if not raw and hasattr(response, "content"):
                raw = response.content.decode("utf-8", errors="replace")
            raw = raw.strip()
            if not raw:
                return []

            peers: list[Any] = []
            decoder = json.JSONDecoder()
            index = 0
            while index < len(raw):
                while index < len(raw) and raw[index].isspace():
                    index += 1
                if index >= len(raw):
                    break
                value, index = decoder.raw_decode(raw, index)
                peers.extend(self._extract_cluster_peers(value))
            return peers

    def _count_unique_cluster_peers(self, peers_payload: list[Any]) -> int:
        """Count unique peer identities without expanding each peer's cluster_peers field."""
        peer_ids: set[str] = set()
        anonymous_count = 0

        for peer in peers_payload:
            if isinstance(peer, dict):
                peer_id = peer.get("id")
                if peer_id:
                    peer_ids.add(str(peer_id))
                else:
                    anonymous_count += 1
            elif peer:
                peer_ids.add(str(peer))

        return len(peer_ids) + anonymous_count

    def _health_payload(
        self,
        *,
        available_cluster_peers: int | None,
        error: str | None,
        checked_at: datetime,
        cached: bool,
    ) -> dict[str, Any]:
        """Build the common health detail payload exposed through Store API."""
        return {
            "available_cluster_peers": available_cluster_peers,
            "min_cluster_peers": self.min_cluster_peers,
            "error": error,
            "cached": cached,
            "checked_at": checked_at.isoformat(),
            "cache_ttl_seconds": self.health_cache_ttl_seconds,
            "add_mode": self.add_mode,
        }

    def _cached_health_is_valid(self, now: datetime) -> bool:
        checked_at_raw = self.last_health.get("checked_at")
        if self._last_health_result is None or not checked_at_raw:
            return False
        if self.health_cache_ttl_seconds <= 0:
            return False

        try:
            checked_at = datetime.fromisoformat(str(checked_at_raw))
        except ValueError:
            return False

        return now - checked_at < timedelta(seconds=self.health_cache_ttl_seconds)

    async def health_check(self, refresh: bool = False) -> bool:
        """Check if IPFS node and Cluster are accessible and have enough peers."""
        now = datetime.now(timezone.utc)
        if not refresh and self._cached_health_is_valid(now):
            self.last_health = {**self.last_health, "cached": True}
            return bool(self._last_health_result)

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Check IPFS node
                ipfs_response = await client.post(f"{self.ipfs_api_url}/api/v0/id")
                if ipfs_response.status_code != 200:
                    logger.error(f"IPFS node unhealthy: {ipfs_response.status_code}")
                    self._last_health_result = False
                    self.last_health = self._health_payload(
                        available_cluster_peers=None,
                        error=f"IPFS node unhealthy: {ipfs_response.status_code}",
                        checked_at=now,
                        cached=False,
                    )
                    return False

                # Check Cluster
                cluster_response = await client.get(f"{self.cluster_api_url}/id")
                if cluster_response.status_code != 200:
                    logger.error(f"Cluster unhealthy: {cluster_response.status_code}")
                    self._last_health_result = False
                    self.last_health = self._health_payload(
                        available_cluster_peers=None,
                        error=f"Cluster unhealthy: {cluster_response.status_code}",
                        checked_at=now,
                        cached=False,
                    )
                    return False

                if self.add_mode == "cluster_proxy":
                    proxy_response = await client.post(f"{self.cluster_proxy_api_url}/api/v0/id")
                    if proxy_response.status_code != 200:
                        logger.error("Cluster Proxy unhealthy: %s", proxy_response.status_code)
                        self._last_health_result = False
                        self.last_health = self._health_payload(
                            available_cluster_peers=None,
                            error=f"Cluster Proxy unhealthy: {proxy_response.status_code}",
                            checked_at=now,
                            cached=False,
                        )
                        return False

                peers_response = await client.get(f"{self.cluster_api_url}/peers")
                if peers_response.status_code != 200:
                    logger.error(f"Cluster peers unavailable: {peers_response.status_code}")
                    self._last_health_result = False
                    self.last_health = self._health_payload(
                        available_cluster_peers=None,
                        error=f"Cluster peers unavailable: {peers_response.status_code}",
                        checked_at=now,
                        cached=False,
                    )
                    return False

                peers_payload = self._parse_cluster_peers_response(peers_response)
                available_peers = self._count_unique_cluster_peers(peers_payload)
                self.last_health = self._health_payload(
                    available_cluster_peers=available_peers,
                    error=None,
                    checked_at=now,
                    cached=False,
                )
                if available_peers < self.min_cluster_peers:
                    logger.error(
                        "Cluster has insufficient peers: available=%s min=%s",
                        available_peers,
                        self.min_cluster_peers,
                    )
                    self.last_health["error"] = (
                        f"not enough cluster peers: available {available_peers}, "
                        f"required {self.min_cluster_peers}"
                    )
                    self._last_health_result = False
                    return False

                self._last_health_result = True
                return True

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            self._last_health_result = False
            self.last_health = self._health_payload(
                available_cluster_peers=None,
                error=str(e),
                checked_at=now,
                cached=False,
            )
            return False
