"""Multi-site IPFS Cluster backend with pooled, balanced local endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any

import httpx

from .base import (
    ContentInfo,
    ContentNotFoundError,
    PinStatus,
    ReplicationInfo,
    ReplicationQuorumError,
    StorageBackend,
    StorageError,
)

logger = logging.getLogger(__name__)


class _EndpointPool:
    """Return rotating endpoint orders and temporarily cool failed endpoints."""

    def __init__(self, urls: list[str], cooldown_seconds: float = 30.0):
        self.urls = tuple(urls)
        self.cooldown_seconds = max(float(cooldown_seconds), 0.0)
        self._next_index = 0
        self._cooldown_until: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def ordered(self) -> list[str]:
        async with self._lock:
            start = self._next_index
            self._next_index = (self._next_index + 1) % len(self.urls)
            rotated = list(self.urls[start:]) + list(self.urls[:start])
            now = monotonic()
            available = [url for url in rotated if self._cooldown_until.get(url, 0.0) <= now]
            return available or rotated

    async def failed(self, url: str) -> None:
        async with self._lock:
            self._cooldown_until[url] = monotonic() + self.cooldown_seconds

    async def succeeded(self, url: str) -> None:
        async with self._lock:
            self._cooldown_until.pop(url, None)


class IPFSClusterBackend(StorageBackend):
    """Use local storage peers while observing one global IPFS Cluster."""

    def __init__(
        self,
        *,
        ipfs_api_urls: list[str],
        cluster_api_urls: list[str],
        cluster_proxy_api_urls: list[str],
        peer_sites: dict[str, str],
        local_site_id: str,
        timeout: float = 30.0,
        health_cache_ttl_seconds: float = 10.0,
        confirmation_timeout_seconds: float = 120.0,
        endpoint_cooldown_seconds: float = 30.0,
    ):
        self.ipfs_api_urls = self._urls(ipfs_api_urls, "IPFS_API_URLS_JSON")
        self.cluster_api_urls = self._urls(cluster_api_urls, "IPFS_CLUSTER_API_URLS_JSON")
        self.cluster_proxy_api_urls = self._urls(
            cluster_proxy_api_urls, "IPFS_CLUSTER_PROXY_API_URLS_JSON"
        )
        self.peer_sites = {str(key): str(value) for key, value in peer_sites.items()}
        self.local_site_id = str(local_site_id).strip()
        if not self.local_site_id:
            raise ValueError("IPFS_CLUSTER_LOCAL_SITE_ID is required")
        known_sites = set(self.peer_sites.values())
        if self.local_site_id not in known_sites:
            raise ValueError("IPFS_CLUSTER_LOCAL_SITE_ID is not present in the peer topology")
        local_peer_count = sum(site == self.local_site_id for site in self.peer_sites.values())
        if local_peer_count != len(self.cluster_proxy_api_urls):
            raise ValueError("local Cluster Proxy endpoints must match local topology peers")

        self.timeout = max(float(timeout), 0.1)
        self.health_cache_ttl_seconds = max(float(health_cache_ttl_seconds), 0.0)
        self.confirmation_timeout_seconds = max(float(confirmation_timeout_seconds), 0.0)
        self.last_health: dict[str, Any] = {}
        self._last_health_result: bool | None = None
        self._client: httpx.AsyncClient | None = None
        self._ipfs_pool = _EndpointPool(self.ipfs_api_urls, endpoint_cooldown_seconds)
        self._cluster_pool = _EndpointPool(self.cluster_api_urls, endpoint_cooldown_seconds)
        self._proxy_pool = _EndpointPool(self.cluster_proxy_api_urls, endpoint_cooldown_seconds)

    @staticmethod
    def _urls(values: list[str], setting: str) -> list[str]:
        urls = [str(value).rstrip("/") for value in values if str(value).strip()]
        if not urls:
            raise ValueError(f"{setting} must contain at least one URL")
        return urls

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _http(self) -> httpx.AsyncClient:
        await self.start()
        assert self._client is not None
        return self._client

    @staticmethod
    def _retryable_status(status_code: int) -> bool:
        return status_code == 429 or status_code >= 500

    async def store(self, content: bytes) -> ContentInfo:
        """Add through alternating local proxies and confirm one real pin."""
        failures: list[str] = []
        info: ContentInfo | None = None
        client = await self._http()
        for base_url in await self._proxy_pool.ordered():
            try:
                response = await client.post(
                    f"{base_url}/api/v0/add",
                    params={"cid-version": 1, "pin": True},
                    files={"file": ("content", content, "application/octet-stream")},
                )
                if self._retryable_status(response.status_code):
                    await self._proxy_pool.failed(base_url)
                    failures.append(f"{base_url}: HTTP {response.status_code}")
                    continue
                response.raise_for_status()
                info = self._parse_ipfs_add_response(response, len(content))
                await self._proxy_pool.succeeded(base_url)
                break
            except httpx.RequestError as exc:
                await self._proxy_pool.failed(base_url)
                failures.append(f"{base_url}: {exc}")
            except (httpx.HTTPStatusError, StorageError) as exc:
                raise StorageError(f"Cluster Proxy add failed at {base_url}: {exc}") from exc

        if info is None:
            raise StorageError("all local Cluster Proxy endpoints failed: " + "; ".join(failures))

        replication = await self._wait_for_first_pin(info.cid)
        info.replication = replication
        logger.info(
            "Stored CID=%s with first pin: total=%s local=%s remote=%s",
            info.cid,
            replication.total_replicas,
            replication.local_replicas,
            replication.remote_replicas,
        )
        return info

    def _parse_ipfs_add_response(self, response: httpx.Response, fallback_size: int) -> ContentInfo:
        try:
            result = response.json()
        except ValueError as exc:
            raise StorageError(f"invalid IPFS add response: {exc}") from exc
        if isinstance(result, list):
            result = result[-1] if result else None
        if not isinstance(result, dict) or not result.get("Hash"):
            raise StorageError("invalid IPFS add response: missing Hash")
        try:
            size = int(result.get("Size", fallback_size))
        except (TypeError, ValueError):
            size = fallback_size
        return ContentInfo(cid=str(result["Hash"]), size=size)

    async def _wait_for_first_pin(self, cid: str) -> ReplicationInfo:
        deadline = monotonic() + self.confirmation_timeout_seconds
        delay = 0.1
        last_total = 0
        while True:
            try:
                snapshot, statuses = await self._replication_snapshot(cid)
                last_total = snapshot.total_replicas
                if last_total >= 1:
                    return snapshot
                if "pin_error" in statuses:
                    logger.warning("Pin error observed while confirming CID=%s", cid)
            except (StorageError, ContentNotFoundError):
                pass
            if monotonic() >= deadline:
                raise ReplicationQuorumError(
                    f"first pin not reached for {cid}: replicas {last_total}/1"
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2.0, 2.0)

    async def _global_status(self, cid: str) -> dict[str, Any]:
        failures: list[str] = []
        not_found = 0
        client = await self._http()
        urls = await self._cluster_pool.ordered()
        for base_url in urls:
            try:
                response = await client.get(f"{base_url}/pins/{cid}")
                if response.status_code == 404:
                    not_found += 1
                    continue
                if self._retryable_status(response.status_code):
                    await self._cluster_pool.failed(base_url)
                    failures.append(f"{base_url}: HTTP {response.status_code}")
                    continue
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict):
                    raise StorageError(f"invalid Cluster status response from {base_url}")
                await self._cluster_pool.succeeded(base_url)
                return result
            except httpx.RequestError as exc:
                await self._cluster_pool.failed(base_url)
                failures.append(f"{base_url}: {exc}")
            except (httpx.HTTPStatusError, ValueError) as exc:
                raise StorageError(f"Cluster status failed at {base_url}: {exc}") from exc
        if not_found == len(urls):
            raise ContentNotFoundError(f"pin not found for CID: {cid}")
        raise StorageError("all local Cluster APIs failed: " + "; ".join(failures))

    def _pinned_distribution(self, result: dict[str, Any]) -> tuple[dict[str, int], set[str]]:
        sites: dict[str, int] = {}
        statuses: set[str] = set()
        peer_map = result.get("peer_map", {})
        if not isinstance(peer_map, dict):
            return sites, statuses
        for peer_id, value in peer_map.items():
            if not isinstance(value, dict):
                continue
            status = str(value.get("status", "unknown")).lower()
            statuses.add(status)
            if status != "pinned":
                continue
            peer_name = str(value.get("peername", ""))
            site = self.peer_sites.get(peer_name) or self.peer_sites.get(str(peer_id))
            if site:
                sites[site] = sites.get(site, 0) + 1
        return sites, statuses

    def _snapshot(self, sites: dict[str, int]) -> ReplicationInfo:
        local = sites.get(self.local_site_id, 0)
        total = sum(sites.values())
        remote = total - local
        all_sites = set(self.peer_sites.values())
        local_target = sum(site == self.local_site_id for site in self.peer_sites.values())
        remote_target = 0 if len(all_sites) == 1 else 1
        site_target = 1 if len(all_sites) == 1 else 2
        purge_target_met = (
            local >= local_target
            and remote >= remote_target
            and len(sites) >= site_target
        )
        return ReplicationInfo(
            total_replicas=total,
            local_replicas=local,
            remote_replicas=remote,
            sites=dict(sorted(sites.items())),
            purge_target_met=purge_target_met,
            checked_at=datetime.now(timezone.utc),
        )

    async def _replication_snapshot(self, cid: str) -> tuple[ReplicationInfo, set[str]]:
        result = await self._global_status(cid)
        sites, statuses = self._pinned_distribution(result)
        return self._snapshot(sites), statuses

    async def retrieve(self, cid: str) -> bytes:
        failures: list[str] = []
        not_found = 0
        client = await self._http()
        urls = await self._ipfs_pool.ordered()
        for base_url in urls:
            try:
                response = await client.post(f"{base_url}/api/v0/cat", params={"arg": cid})
                if response.status_code == 500 and any(
                    marker in response.text.lower() for marker in ("not found", "no link")
                ):
                    not_found += 1
                    continue
                if self._retryable_status(response.status_code):
                    await self._ipfs_pool.failed(base_url)
                    failures.append(f"{base_url}: HTTP {response.status_code}")
                    continue
                response.raise_for_status()
                await self._ipfs_pool.succeeded(base_url)
                return response.content
            except httpx.RequestError as exc:
                await self._ipfs_pool.failed(base_url)
                failures.append(f"{base_url}: {exc}")
            except httpx.HTTPStatusError as exc:
                raise StorageError(f"IPFS retrieve failed at {base_url}: {exc}") from exc
        if not_found == len(urls):
            raise ContentNotFoundError(f"content not found for CID: {cid}")
        raise StorageError("all local IPFS APIs failed: " + "; ".join(failures))

    async def status(self, cid: str) -> PinStatus:
        replication, statuses = await self._replication_snapshot(cid)
        if replication.total_replicas:
            overall = "pinned"
        elif "pinning" in statuses:
            overall = "pinning"
        elif any("error" in value for value in statuses):
            overall = "error"
        else:
            overall = "unpinned"
        return PinStatus(cid=cid, status=overall, replication=replication)

    @staticmethod
    def _extract_peers(response: httpx.Response) -> list[dict[str, Any]]:
        try:
            values = response.json()
            if isinstance(values, dict):
                values = values.get("peers", [values])
            return [value for value in values if isinstance(value, dict)]
        except ValueError:
            raw = response.text.strip()
            peers: list[dict[str, Any]] = []
            decoder = json.JSONDecoder()
            index = 0
            while index < len(raw):
                while index < len(raw) and raw[index].isspace():
                    index += 1
                if index >= len(raw):
                    break
                value, index = decoder.raw_decode(raw, index)
                if isinstance(value, dict):
                    peers.append(value)
            return peers

    async def _probe_any(self, method: str, pool: _EndpointPool, path: str) -> bool:
        client = await self._http()
        for base_url in await pool.ordered():
            try:
                response = await getattr(client, method)(f"{base_url}{path}", timeout=5.0)
                if response.status_code == 200:
                    await pool.succeeded(base_url)
                    return True
                if self._retryable_status(response.status_code):
                    await pool.failed(base_url)
            except httpx.RequestError:
                await pool.failed(base_url)
        return False

    async def read_health_check(self) -> bool:
        healthy = await self._probe_any("post", self._ipfs_pool, "/api/v0/id")
        self.last_health = {
            "dimension": "read",
            "error": None if healthy else "no local Kubo endpoint is healthy",
        }
        return healthy

    async def write_health_check(self, refresh: bool = False) -> bool:
        now = datetime.now(timezone.utc)
        checked_at = self.last_health.get("checked_at")
        if (
            not refresh
            and self._last_health_result is not None
            and checked_at
            and self.health_cache_ttl_seconds > 0
        ):
            try:
                if now - datetime.fromisoformat(str(checked_at)) < timedelta(
                    seconds=self.health_cache_ttl_seconds
                ):
                    self.last_health = {**self.last_health, "cached": True}
                    return self._last_health_result
            except ValueError:
                pass

        kubo_ok = await self._probe_any("post", self._ipfs_pool, "/api/v0/id")
        proxy_ok = await self._probe_any("post", self._proxy_pool, "/api/v0/id")
        peers: list[dict[str, Any]] = []
        client = await self._http()
        for base_url in await self._cluster_pool.ordered():
            try:
                response = await client.get(f"{base_url}/peers", timeout=5.0)
                if response.status_code == 200:
                    peers = self._extract_peers(response)
                    await self._cluster_pool.succeeded(base_url)
                    break
                if self._retryable_status(response.status_code):
                    await self._cluster_pool.failed(base_url)
            except httpx.RequestError:
                await self._cluster_pool.failed(base_url)

        available_names = {
            str(peer.get("peername") or peer.get("id"))
            for peer in peers
            if (peer.get("peername") or peer.get("id")) and not peer.get("error")
        }
        available_sites = {
            site for name in available_names if (site := self.peer_sites.get(name))
        }
        healthy = kubo_ok and proxy_ok and bool(available_names) and bool(available_sites)
        errors = []
        if not kubo_ok:
            errors.append("no local Kubo endpoint is healthy")
        if not proxy_ok:
            errors.append("no local Cluster Proxy endpoint is healthy")
        if not available_names:
            errors.append("no Cluster peer is visible")
        if not available_sites:
            errors.append("no Cluster site is visible")
        self.last_health = {
            "dimension": "write",
            "available_cluster_peers": len(available_names),
            "available_cluster_sites": len(available_sites),
            "min_cluster_peers": 1,
            "min_cluster_sites": 1,
            "expected_cluster_peers": len(self.peer_sites),
            "error": "; ".join(errors) or None,
            "cached": False,
            "checked_at": now.isoformat(),
            "cache_ttl_seconds": self.health_cache_ttl_seconds,
        }
        self._last_health_result = healthy
        return healthy

    async def health_check(self, refresh: bool = False) -> bool:
        return await self.write_health_check(refresh=refresh)
