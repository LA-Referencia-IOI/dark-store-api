"""Tests for filesystem and global IPFS Cluster backends."""

import json
import tempfile

import httpx
import pytest

from app.backends.base import ContentNotFoundError, ReplicationQuorumError
from app.backends.filesystem import FileSystemBackend
from app.backends.ipfs_cluster import IPFSClusterBackend


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=None, content=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else (json.dumps(payload) if payload is not None else "")
        self.content = content if content is not None else self.text.encode()

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

    async def _call(self, method, url, kwargs):
        self.calls.append((method, url, kwargs))
        result = self.routes[(method, url)]
        if isinstance(result, Exception):
            raise result
        return result

    async def post(self, url, **kwargs):
        return await self._call("POST", url, kwargs)

    async def get(self, url, **kwargs):
        return await self._call("GET", url, kwargs)


class TestFileSystemBackend:
    @pytest.fixture
    def backend(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FileSystemBackend(tmpdir)

    @pytest.mark.asyncio
    async def test_store_retrieve_status_and_health(self, backend):
        info = await backend.store(b"content")
        assert await backend.retrieve(info.cid) == b"content"
        status = await backend.status(info.cid)
        assert status.pinned and status.replicas == 1
        assert await backend.health_check()

    @pytest.mark.asyncio
    async def test_unknown_content(self, backend):
        with pytest.raises(ContentNotFoundError):
            await backend.retrieve("unknown")


def backend(**overrides):
    values = {
        "ipfs_api_urls": ["http://ipfs-a", "http://ipfs-b"],
        "cluster_api_urls": ["http://cluster-a", "http://cluster-b"],
        "cluster_proxy_api_urls": ["http://proxy-a", "http://proxy-b"],
        "peer_sites": {
            "site-a-storage-1": "site-a",
            "site-a-storage-2": "site-a",
            "site-b-storage-1": "site-b",
        },
        "expected_cluster_peers": 3,
        "write_min_peers": 2,
        "write_min_sites": 2,
        "confirmation_timeout_seconds": 0,
    }
    values.update(overrides)
    return IPFSClusterBackend(**values)


class TestIPFSClusterBackend:
    @pytest.fixture(autouse=True)
    def fake_http(self, monkeypatch):
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {}
        monkeypatch.setattr("app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient)

    @pytest.mark.asyncio
    async def test_store_fails_over_and_waits_for_peer_and_site_quorum(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy-a/api/v0/add"): _FakeResponse(503, text="down"),
            ("POST", "http://proxy-b/api/v0/add"): _FakeResponse(200, {"Hash": "bafy", "Size": "4"}),
            ("GET", "http://cluster-a/pins/bafy"): _FakeResponse(200, {
                "peer_map": {
                    "peer-a": {"peername": "site-a-storage-1", "status": "pinned"},
                    "peer-b": {"peername": "site-b-storage-1", "status": "pinned"},
                }
            }),
        }

        info = await backend().store(b"data")

        assert info.cid == "bafy"
        assert info.replication.status == "durable"
        assert info.replication.pinned_peers == 2
        assert info.replication.pinned_sites == 2
        assert [call[1] for call in _FakeAsyncClient.calls[:2]] == [
            "http://proxy-a/api/v0/add",
            "http://proxy-b/api/v0/add",
        ]

    @pytest.mark.asyncio
    async def test_store_returns_retryable_error_when_quorum_is_not_observed(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy-a/api/v0/add"): _FakeResponse(200, {"Hash": "bafy", "Size": "4"}),
            ("GET", "http://cluster-a/pins/bafy"): _FakeResponse(200, {
                "peer_map": {
                    "peer-a": {"peername": "site-a-storage-1", "status": "pinned"},
                }
            }),
        }

        with pytest.raises(ReplicationQuorumError, match="peers 1/2, sites 1/2"):
            await backend().store(b"data")

    @pytest.mark.asyncio
    async def test_retrieve_fails_over_to_second_local_kubo(self):
        request = httpx.Request("POST", "http://ipfs-a/api/v0/cat")
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/cat"): httpx.ConnectError("down", request=request),
            ("POST", "http://ipfs-b/api/v0/cat"): _FakeResponse(200, content=b"payload"),
        }

        assert await backend().retrieve("bafy") == b"payload"

    @pytest.mark.asyncio
    async def test_read_health_needs_only_one_local_kubo(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(503),
            ("POST", "http://ipfs-b/api/v0/id"): _FakeResponse(200, {"ID": "kubo"}),
        }
        instance = backend()

        assert await instance.read_health_check()
        assert instance.last_health["dimension"] == "read"

    @pytest.mark.asyncio
    async def test_write_health_requires_peer_and_site_quorum(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(200, {"ID": "kubo"}),
            ("POST", "http://proxy-a/api/v0/id"): _FakeResponse(200, {"ID": "proxy"}),
            ("GET", "http://cluster-a/peers"): _FakeResponse(200, [
                {"id": "peer-a", "peername": "site-a-storage-1"},
                {"id": "peer-b", "peername": "site-b-storage-1"},
            ]),
        }
        instance = backend()

        assert await instance.write_health_check()
        assert instance.last_health["available_cluster_peers"] == 2
        assert instance.last_health["available_cluster_sites"] == 2

    @pytest.mark.asyncio
    async def test_write_health_fails_when_peers_are_only_in_one_site(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(200),
            ("POST", "http://proxy-a/api/v0/id"): _FakeResponse(200),
            ("GET", "http://cluster-a/peers"): _FakeResponse(
                200,
                text=(
                    '{"id":"peer-a","peername":"site-a-storage-1"}\n'
                    '{"id":"peer-b","peername":"site-a-storage-2"}\n'
                ),
            ),
        }
        instance = backend()

        assert not await instance.write_health_check()
        assert "cluster sites 1/2" in instance.last_health["error"]
