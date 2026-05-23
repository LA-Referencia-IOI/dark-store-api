"""
Tests for storage backends.
"""

import json
import os
import tempfile

import httpx
import pytest

from app.backends.filesystem import FileSystemBackend
from app.backends.base import ContentNotFoundError, StorageError
from app.backends.ipfs_cluster import IPFSClusterBackend


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=None):
        self.status_code = status_code
        self._payload = payload
        if text is not None:
            self.text = text
        elif payload is not None:
            self.text = json.dumps(payload)
        else:
            self.text = ""
        self.content = self.text.encode("utf-8")

    def json(self):
        if self._payload is not None:
            return self._payload
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://test")
            response = httpx.Response(self.status_code, request=request, text=self.text)
            raise httpx.HTTPStatusError("HTTP error", request=request, response=response)


class _FakeAsyncClient:
    routes = {}
    calls = []

    def __init__(self, *args, **kwargs):
        del args, kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.routes[("POST", url)]

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.routes[("GET", url)]


class TestFileSystemBackend:
    """Tests for FileSystemBackend."""

    @pytest.fixture
    def backend(self) -> FileSystemBackend:
        """Create a backend with temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FileSystemBackend(tmpdir)

    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, backend: FileSystemBackend):
        """Store and retrieve content."""
        content = b'{"test": "data"}'

        info = await backend.store(content)

        assert info.cid is not None
        assert info.size == len(content)

        # Retrieve
        retrieved = await backend.retrieve(info.cid)

        assert retrieved == content

    @pytest.mark.asyncio
    async def test_content_addressable(self, backend: FileSystemBackend):
        """Same content produces same CID."""
        content = b"reproducible content"

        info1 = await backend.store(content)
        info2 = await backend.store(content)

        assert info1.cid == info2.cid

    @pytest.mark.asyncio
    async def test_retrieve_not_found(self, backend: FileSystemBackend):
        """Retrieve non-existent CID raises error."""
        with pytest.raises(ContentNotFoundError):
            await backend.retrieve("nonexistent123")

    @pytest.mark.asyncio
    async def test_status_pinned(self, backend: FileSystemBackend):
        """Status shows pinned for stored content."""
        info = await backend.store(b"test")

        status = await backend.status(info.cid)

        assert status.cid == info.cid
        assert status.pinned is True
        assert status.status == "pinned"
        assert status.replicas == 1  # Filesystem has single replica

    @pytest.mark.asyncio
    async def test_status_not_found(self, backend: FileSystemBackend):
        """Status for unknown CID raises error."""
        with pytest.raises(ContentNotFoundError):
            await backend.status("unknowncid")

    @pytest.mark.asyncio
    async def test_health_check(self, backend: FileSystemBackend):
        """Health check returns True for valid directory."""
        result = await backend.health_check()
        assert result is True

    @pytest.mark.asyncio
    async def test_binary_content(self, backend: FileSystemBackend):
        """Store and retrieve binary content."""
        content = bytes(range(256))  # All byte values

        info = await backend.store(content)
        retrieved = await backend.retrieve(info.cid)

        assert retrieved == content


class TestIPFSClusterBackendHealth:
    """Tests for IPFS Cluster readiness checks."""

    @pytest.mark.asyncio
    async def test_store_uses_cluster_proxy_without_cluster_pin(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            cluster_proxy_api_url="http://proxy:9095",
            add_mode="cluster_proxy",
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy:9095/api/v0/add"): _FakeResponse(
                200,
                {"Hash": "bafkproxy", "Size": "4"},
            ),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        info = await backend.store(b"data")

        assert info.cid == "bafkproxy"
        assert info.size == 4
        assert [call[1] for call in _FakeAsyncClient.calls] == ["http://proxy:9095/api/v0/add"]
        assert _FakeAsyncClient.calls[0][2]["params"]["pin"] is True

    @pytest.mark.asyncio
    async def test_store_legacy_ipfs_then_cluster_uses_pin_false(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            cluster_proxy_api_url="http://proxy:9095",
            add_mode="ipfs_then_cluster",
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/add"): _FakeResponse(
                200,
                {"Hash": "bafklegacy", "Size": "4"},
            ),
            ("POST", "http://cluster:9094/pins/bafklegacy"): _FakeResponse(202, {}),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        info = await backend.store(b"data")

        assert info.cid == "bafklegacy"
        assert [call[1] for call in _FakeAsyncClient.calls] == [
            "http://ipfs:5001/api/v0/add",
            "http://cluster:9094/pins/bafklegacy",
        ]
        assert _FakeAsyncClient.calls[0][2]["params"]["pin"] is False

    @pytest.mark.asyncio
    async def test_store_cluster_proxy_failure_is_storage_error(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            cluster_proxy_api_url="http://proxy:9095",
            add_mode="cluster_proxy",
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy:9095/api/v0/add"): _FakeResponse(500, text="not enough peers"),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        with pytest.raises(StorageError) as exc:
            await backend.store(b"data")

        assert "Cluster Proxy storage failed" in str(exc.value)

    @pytest.mark.asyncio
    async def test_health_check_requires_min_cluster_peers(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            min_cluster_peers=2,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://localhost:9095/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster:9094/peers"): _FakeResponse(200, [{"id": "peer-1"}]),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is False
        assert backend.last_health["available_cluster_peers"] == 1
        assert backend.last_health["min_cluster_peers"] == 2
        assert "not enough cluster peers" in backend.last_health["error"]

    @pytest.mark.asyncio
    async def test_health_check_passes_with_enough_cluster_peers(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            min_cluster_peers=2,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://localhost:9095/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster:9094/peers"): _FakeResponse(200, [{"id": "peer-1"}, {"id": "peer-2"}]),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is True
        assert backend.last_health["available_cluster_peers"] == 2
        assert backend.last_health["min_cluster_peers"] == 2
        assert backend.last_health["error"] is None

    @pytest.mark.asyncio
    async def test_health_check_requires_cluster_proxy_when_proxy_mode(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            cluster_proxy_api_url="http://proxy:9095",
            add_mode="cluster_proxy",
            min_cluster_peers=2,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://proxy:9095/api/v0/id"): _FakeResponse(503, text="proxy down"),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is False
        assert backend.last_health["error"] == "Cluster Proxy unhealthy: 503"

    @pytest.mark.asyncio
    async def test_health_check_passes_with_cluster_peers_json_stream(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            min_cluster_peers=2,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://localhost:9095/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster:9094/peers"): _FakeResponse(
                200,
                text='{"id": "peer-1"}\n{"id": "peer-2"}\n',
            ),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is True
        assert backend.last_health["available_cluster_peers"] == 2
        assert backend.last_health["min_cluster_peers"] == 2
        assert backend.last_health["error"] is None

    @pytest.mark.asyncio
    async def test_health_check_counts_unique_peer_ids_in_json_stream(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            min_cluster_peers=2,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://localhost:9095/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster:9094/peers"): _FakeResponse(
                200,
                text=(
                    '{"id": "peer-1", "cluster_peers": ["peer-1", "peer-2"]}\n'
                    '{"id": "peer-2", "cluster_peers": ["peer-1", "peer-2"]}\n'
                    '{"id": "peer-1", "cluster_peers": ["peer-1", "peer-2"]}\n'
                ),
            ),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is True
        assert backend.last_health["available_cluster_peers"] == 2

    @pytest.mark.asyncio
    async def test_health_check_uses_cache_until_refresh(self, monkeypatch):
        backend = IPFSClusterBackend(
            ipfs_api_url="http://ipfs:5001",
            cluster_api_url="http://cluster:9094",
            min_cluster_peers=2,
            health_cache_ttl_seconds=60,
        )
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs:5001/api/v0/id"): _FakeResponse(200, {"ID": "ipfs"}),
            ("GET", "http://cluster:9094/id"): _FakeResponse(200, {"id": "cluster"}),
            ("POST", "http://localhost:9095/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster:9094/peers"): _FakeResponse(
                200,
                [{"id": "peer-1"}, {"id": "peer-2"}],
            ),
        }
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

        assert await backend.health_check() is True
        assert await backend.health_check() is True
        assert len(_FakeAsyncClient.calls) == 4
        assert backend.last_health["cached"] is True

        assert await backend.health_check(refresh=True) is True
        assert len(_FakeAsyncClient.calls) == 8
        assert backend.last_health["cached"] is False
