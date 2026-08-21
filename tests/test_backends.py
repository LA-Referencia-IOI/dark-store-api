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
    instances = []

    def __init__(self, *args, **kwargs):
        del args, kwargs
        self.closed = False
        self.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb

    async def aclose(self):
        self.closed = True

    async def _call(self, method, url, kwargs):
        self.calls.append((method, url, kwargs))
        result = self.routes[(method, url)]
        if isinstance(result, list):
            result = result.pop(0)
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
        assert status.status == "pinned"
        assert status.replication.total_replicas == 1
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
        "local_site_id": "site-a",
        "confirmation_timeout_seconds": 0,
    }
    values.update(overrides)
    return IPFSClusterBackend(**values)


class TestIPFSClusterBackend:
    @pytest.fixture(autouse=True)
    def fake_http(self, monkeypatch):
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {}
        _FakeAsyncClient.instances = []
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
        assert info.replication.total_replicas == 2
        assert info.replication.local_replicas == 1
        assert info.replication.remote_replicas == 1
        assert [call[1] for call in _FakeAsyncClient.calls[:2]] == [
            "http://proxy-a/api/v0/add",
            "http://proxy-b/api/v0/add",
        ]

    @pytest.mark.asyncio
    async def test_store_accepts_the_first_observed_pin(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy-a/api/v0/add"): _FakeResponse(200, {"Hash": "bafy", "Size": "4"}),
            ("GET", "http://cluster-a/pins/bafy"): _FakeResponse(200, {
                "peer_map": {
                    "peer-a": {"peername": "site-a-storage-1", "status": "pinned"},
                }
            }),
        }

        info = await backend().store(b"data")
        assert info.replication.total_replicas == 1
        assert not info.replication.purge_target_met

    def test_single_site_purge_requires_both_local_pins(self):
        instance = backend(peer_sites={
            "site-a-storage-1": "site-a",
            "site-a-storage-2": "site-a",
        })
        assert not instance._snapshot({"site-a": 1}).purge_target_met
        assert instance._snapshot({"site-a": 2}).purge_target_met

    def test_multisite_purge_requires_two_local_and_one_remote(self):
        instance = backend()
        assert not instance._snapshot({"site-a": 2}).purge_target_met
        assert not instance._snapshot({"site-a": 1, "site-b": 2}).purge_target_met
        assert instance._snapshot({"site-a": 2, "site-b": 1}).purge_target_met

    def test_replica_count_includes_only_pinned_states(self):
        instance = backend()
        sites, statuses = instance._pinned_distribution({
            "peer_map": {
                "one": {"peername": "site-a-storage-1", "status": "pinned"},
                "two": {"peername": "site-a-storage-2", "status": "pinning"},
                "three": {"peername": "site-b-storage-1", "status": "pin_error"},
            }
        })
        assert sites == {"site-a": 1}
        assert statuses == {"pinned", "pinning", "pin_error"}

    @pytest.mark.asyncio
    async def test_store_round_robin_rotates_the_first_proxy(self):
        pinned = _FakeResponse(200, {
            "peer_map": {
                "peer-a": {"peername": "site-a-storage-1", "status": "pinned"},
            }
        })
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy-a/api/v0/add"): _FakeResponse(200, {"Hash": "bafy-a", "Size": "1"}),
            ("POST", "http://proxy-b/api/v0/add"): _FakeResponse(200, {"Hash": "bafy-b", "Size": "1"}),
            ("GET", "http://cluster-a/pins/bafy-a"): pinned,
            ("GET", "http://cluster-b/pins/bafy-b"): pinned,
        }
        instance = backend()
        await instance.store(b"a")
        await instance.store(b"b")
        add_urls = [url for method, url, _ in _FakeAsyncClient.calls if method == "POST" and url.endswith("/add")]
        assert add_urls == ["http://proxy-a/api/v0/add", "http://proxy-b/api/v0/add"]

    @pytest.mark.asyncio
    async def test_failed_proxy_cools_down_then_rejoins_round_robin(self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr("app.backends.ipfs_cluster.monotonic", lambda: now[0])
        pinned = lambda name: _FakeResponse(200, {
            "peer_map": {
                "peer-a": {"peername": "site-a-storage-1", "status": "pinned"},
            }
        })
        request = httpx.Request("POST", "http://proxy-a/api/v0/add")
        _FakeAsyncClient.routes = {
            ("POST", "http://proxy-a/api/v0/add"): [
                httpx.ConnectError("down", request=request),
                _FakeResponse(200, {"Hash": "bafy-c", "Size": "1"}),
            ],
            ("POST", "http://proxy-b/api/v0/add"): [
                _FakeResponse(200, {"Hash": "bafy-a", "Size": "1"}),
                _FakeResponse(200, {"Hash": "bafy-b", "Size": "1"}),
            ],
            ("GET", "http://cluster-a/pins/bafy-a"): pinned("a"),
            ("GET", "http://cluster-b/pins/bafy-b"): pinned("b"),
            ("GET", "http://cluster-a/pins/bafy-c"): pinned("c"),
        }
        instance = backend(endpoint_cooldown_seconds=30)

        await instance.store(b"a")
        await instance.store(b"b")
        now[0] += 31
        await instance.store(b"c")

        add_urls = [url for method, url, _ in _FakeAsyncClient.calls if url.endswith("/add")]
        assert add_urls == [
            "http://proxy-a/api/v0/add",
            "http://proxy-b/api/v0/add",
            "http://proxy-b/api/v0/add",
            "http://proxy-a/api/v0/add",
        ]

    @pytest.mark.asyncio
    async def test_reuses_and_closes_one_http_client(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/cat"): _FakeResponse(200, content=b"payload"),
            ("POST", "http://ipfs-b/api/v0/cat"): _FakeResponse(200, content=b"payload"),
        }
        instance = backend()
        await instance.retrieve("one")
        await instance.retrieve("two")

        assert len(_FakeAsyncClient.instances) == 1
        assert not _FakeAsyncClient.instances[0].closed
        await instance.close()
        assert _FakeAsyncClient.instances[0].closed

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

        assert await instance.write_health_check()
        assert instance.last_health["available_cluster_sites"] == 1
