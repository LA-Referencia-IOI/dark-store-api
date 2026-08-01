"""Multi-site IPFS Cluster backend with site-local endpoint failover."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
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


class IPFSClusterBackend(StorageBackend):
    """Use two local storage peers while observing one global Cluster."""

    def __init__(
        self,
        *,
        ipfs_api_urls: list[str],
        cluster_api_urls: list[str],
        cluster_proxy_api_urls: list[str],
        peer_sites: dict[str, str],
        add_mode: str = "cluster_proxy",
        timeout: float = 30.0,
        health_cache_ttl_seconds: float = 10.0,
        expected_cluster_peers: int = 2,
        write_min_peers: int = 1,
        write_min_sites: int = 1,
        confirmation_timeout_seconds: float = 120.0,
        confirmation_interval_seconds: float = 2.0,
    ):
        self.ipfs_api_urls = self._urls(ipfs_api_urls, "IPFS_API_URLS_JSON")
        self.cluster_api_urls = self._urls(cluster_api_urls, "IPFS_CLUSTER_API_URLS_JSON")
        self.cluster_proxy_api_urls = self._urls(
            cluster_proxy_api_urls, "IPFS_CLUSTER_PROXY_API_URLS_JSON"
        )
        self.peer_sites = {str(key): str(value) for key, value in peer_sites.items()}
        self.add_mode = add_mode.strip().lower()
        if self.add_mode != "cluster_proxy":
            raise ValueError("IPFS_ADD_MODE must be 'cluster_proxy'")
        self.timeout = timeout
        self.health_cache_ttl_seconds = max(float(health_cache_ttl_seconds), 0.0)
        self.expected_cluster_peers = max(int(expected_cluster_peers), 1)
        self.write_min_peers = max(int(write_min_peers), 1)
        self.write_min_sites = max(int(write_min_sites), 1)
        self.confirmation_timeout_seconds = max(float(confirmation_timeout_seconds), 0.0)
        self.confirmation_interval_seconds = max(float(confirmation_interval_seconds), 0.05)
        self.last_health: dict[str, Any] = {}
        self._last_health_result: bool | None = None

        if self.write_min_peers > self.expected_cluster_peers:
            raise ValueError("write_min_peers cannot exceed expected_cluster_peers")
        if self.write_min_sites > len(set(self.peer_sites.values())):
            raise ValueError("write_min_sites exceeds the configured topology sites")

    @staticmethod
    def _urls(values: list[str], setting: str) -> list[str]:
        urls = [str(value).rstrip("/") for value in values if str(value).strip()]
        if not urls:
            raise ValueError(f"{setting} must contain at least one URL")
        return urls

    async def store(self, content: bytes) -> ContentInfo:
        """Add through a local Cluster Proxy and wait for durable quorum."""
        failures: list[str] = []
        info: ContentInfo | None = None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for base_url in self.cluster_proxy_api_urls:
                try:
                    response = await client.post(
                        f"{base_url}/api/v0/add",
                        params={"cid-version": 1, "pin": True},
                        files={"file": ("content", content, "application/octet-stream")},
                    )
                    response.raise_for_status()
                    info = self._parse_ipfs_add_response(response, len(content))
                    break
                except (httpx.HTTPError, StorageError) as exc:
                    failures.append(f"{base_url}: {exc}")

        if info is None:
            raise StorageError("all local Cluster Proxy endpoints failed: " + "; ".join(failures))

        replication = await self._wait_for_replication(info.cid)
        info.replication = replication
        logger.info(
            "Stored CID=%s with durable quorum: peers=%s sites=%s",
            info.cid,
            replication.pinned_peers,
            replication.pinned_sites,
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

    async def _wait_for_replication(self, cid: str) -> ReplicationInfo:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.confirmation_timeout_seconds
        last_peers = 0
        last_sites = 0
        while True:
            try:
                status = await self._global_status(cid)
                pinned_ids, sites = self._pinned_locations(status)
                last_peers, last_sites = len(pinned_ids), len(sites)
                if last_peers >= self.write_min_peers and last_sites >= self.write_min_sites:
                    return ReplicationInfo(
                        status="durable",
                        pinned_peers=last_peers,
                        pinned_sites=last_sites,
                        target_peers=self.expected_cluster_peers,
                    )
            except (StorageError, ContentNotFoundError):
                pass
            if loop.time() >= deadline:
                raise ReplicationQuorumError(
                    f"replication quorum not reached for {cid}: "
                    f"pinned peers {last_peers}/{self.write_min_peers}, "
                    f"sites {last_sites}/{self.write_min_sites}"
                )
            await asyncio.sleep(self.confirmation_interval_seconds)

    async def _global_status(self, cid: str) -> dict[str, Any]:
        failures: list[str] = []
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for base_url in self.cluster_api_urls:
                try:
                    response = await client.get(f"{base_url}/pins/{cid}")
                    if response.status_code == 404:
                        failures.append(f"{base_url}: not found")
                        continue
                    response.raise_for_status()
                    result = response.json()
                    if isinstance(result, dict):
                        return result
                    failures.append(f"{base_url}: invalid JSON shape")
                except (httpx.HTTPError, ValueError) as exc:
                    failures.append(f"{base_url}: {exc}")
        if failures and all("not found" in failure for failure in failures):
            raise ContentNotFoundError(f"pin not found for CID: {cid}")
        raise StorageError("all local Cluster APIs failed: " + "; ".join(failures))

    def _pinned_locations(self, result: dict[str, Any]) -> tuple[set[str], set[str]]:
        pinned: set[str] = set()
        sites: set[str] = set()
        peer_map = result.get("peer_map", {})
        if not isinstance(peer_map, dict):
            return pinned, sites
        for peer_id, value in peer_map.items():
            if not isinstance(value, dict) or str(value.get("status", "")).lower() != "pinned":
                continue
            peer_id = str(peer_id)
            pinned.add(peer_id)
            peer_name = str(value.get("peername", ""))
            site = self.peer_sites.get(peer_name) or self.peer_sites.get(peer_id)
            if site:
                sites.add(site)
        return pinned, sites

    async def retrieve(self, cid: str) -> bytes:
        failures: list[str] = []
        not_found = 0
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for base_url in self.ipfs_api_urls:
                try:
                    response = await client.post(f"{base_url}/api/v0/cat", params={"arg": cid})
                    if response.status_code == 500 and any(
                        marker in response.text.lower() for marker in ("not found", "no link")
                    ):
                        not_found += 1
                        continue
                    response.raise_for_status()
                    return response.content
                except httpx.HTTPError as exc:
                    failures.append(f"{base_url}: {exc}")
        if not_found == len(self.ipfs_api_urls):
            raise ContentNotFoundError(f"content not found for CID: {cid}")
        raise StorageError("all local IPFS APIs failed: " + "; ".join(failures))

    async def status(self, cid: str) -> PinStatus:
        result = await self._global_status(cid)
        pinned, _ = self._pinned_locations(result)
        statuses = [
            str(value.get("status", "unknown")).lower()
            for value in result.get("peer_map", {}).values()
            if isinstance(value, dict)
        ]
        if pinned:
            overall = "pinned"
        elif "pinning" in statuses:
            overall = "pinning"
        elif any("error" in value for value in statuses):
            overall = "error"
        else:
            overall = "unpinned"
        return PinStatus(cid=cid, pinned=bool(pinned), replicas=len(pinned), status=overall)

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

    async def _probe_any(self, method: str, urls: list[str], path: str) -> bool:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for base_url in urls:
                try:
                    response = await getattr(client, method)(f"{base_url}{path}")
                    if response.status_code == 200:
                        return True
                except httpx.HTTPError:
                    continue
        return False

    async def read_health_check(self) -> bool:
        healthy = await self._probe_any("post", self.ipfs_api_urls, "/api/v0/id")
        self.last_health = {"dimension": "read", "error": None if healthy else "no local Kubo endpoint is healthy"}
        return healthy

    async def write_health_check(self, refresh: bool = False) -> bool:
        now = datetime.now(timezone.utc)
        checked_at = self.last_health.get("checked_at")
        if not refresh and self._last_health_result is not None and checked_at and self.health_cache_ttl_seconds > 0:
            try:
                if now - datetime.fromisoformat(str(checked_at)) < timedelta(seconds=self.health_cache_ttl_seconds):
                    self.last_health = {**self.last_health, "cached": True}
                    return self._last_health_result
            except ValueError:
                pass

        kubo_ok = await self._probe_any("post", self.ipfs_api_urls, "/api/v0/id")
        proxy_ok = await self._probe_any("post", self.cluster_proxy_api_urls, "/api/v0/id")
        peers: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=5.0) as client:
            for base_url in self.cluster_api_urls:
                try:
                    response = await client.get(f"{base_url}/peers")
                    if response.status_code == 200:
                        peers = self._extract_peers(response)
                        break
                except httpx.HTTPError:
                    continue

        available_names = {
            str(peer.get("peername") or peer.get("id"))
            for peer in peers
            if (peer.get("peername") or peer.get("id")) and not peer.get("error")
        }
        available_sites = {
            site
            for name in available_names
            if (site := self.peer_sites.get(name))
        }
        healthy = (
            kubo_ok
            and proxy_ok
            and len(available_names) >= self.write_min_peers
            and len(available_sites) >= self.write_min_sites
        )
        errors = []
        if not kubo_ok:
            errors.append("no local Kubo endpoint is healthy")
        if not proxy_ok:
            errors.append("no local Cluster Proxy endpoint is healthy")
        if len(available_names) < self.write_min_peers:
            errors.append(f"cluster peers {len(available_names)}/{self.write_min_peers}")
        if len(available_sites) < self.write_min_sites:
            errors.append(f"cluster sites {len(available_sites)}/{self.write_min_sites}")
        self.last_health = {
            "dimension": "write",
            "available_cluster_peers": len(available_names),
            "available_cluster_sites": len(available_sites),
            "min_cluster_peers": self.write_min_peers,
            "min_cluster_sites": self.write_min_sites,
            "expected_cluster_peers": self.expected_cluster_peers,
            "error": "; ".join(errors) or None,
            "cached": False,
            "checked_at": now.isoformat(),
            "cache_ttl_seconds": self.health_cache_ttl_seconds,
            "add_mode": self.add_mode,
        }
        self._last_health_result = healthy
        return healthy

    async def health_check(self, refresh: bool = False) -> bool:
        """Compatibility with the backend interface: health means write readiness."""
        return await self.write_health_check(refresh=refresh)
