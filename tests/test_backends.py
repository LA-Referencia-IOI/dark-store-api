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
        self.headers = {"content-type": "application/json"}
        self.text = (
            text
            if text is not None
            else (json.dumps(payload) if payload is not None else "")
        )
        self.content = content if content is not None else self.text.encode()

    def json(self):
        if self._payload is not None:
            return self._payload
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://test")
            response = httpx.Response(self.status_code, request=request, text=self.text)
            raise httpx.HTTPStatusError(
                "HTTP error", request=request, response=response
            )


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
    }
    values.update(overrides)
    return IPFSClusterBackend(**values)


class TestIPFSClusterBackend:
    @pytest.fixture(autouse=True)
    def fake_http(self, monkeypatch):
        _FakeAsyncClient.calls = []
        _FakeAsyncClient.routes = {}
        _FakeAsyncClient.instances = []
        monkeypatch.setattr(
            "app.backends.ipfs_cluster.httpx.AsyncClient", _FakeAsyncClient
        )

    @pytest.mark.asyncio
    async def test_store_fails_over_and_returns_cid_without_waiting(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): _FakeResponse(503, text="down"),
            ("POST", "http://cluster-b/add"): _FakeResponse(
                200, {"Hash": "bafy", "Size": "4"}
            ),
        }

        info = await backend().store(b"data")

        assert info.cid == "bafy"
        assert info.replication is None
        assert [call[1] for call in _FakeAsyncClient.calls[:2]] == [
            "http://cluster-a/add",
            "http://cluster-b/add",
        ]

    @pytest.mark.asyncio
    async def test_store_accepts_cid_before_pin_is_observed(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): _FakeResponse(
                200, {"Hash": "bafy", "Size": "4"}
            ),
        }

        info = await backend().store(b"data")
        assert info.replication is None
        add_call = next(call for call in _FakeAsyncClient.calls if call[1].endswith("/add"))
        assert add_call[2]["params"]["replication-min"] == 1
        assert add_call[2]["params"]["replication-max"] == 1

    @pytest.mark.asyncio
    async def test_ensure_replication_promotes_without_uploading(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/pins/bafy"): _FakeResponse(200, {"cid": "bafy"}),
        }
        result = await backend().ensure_replication(["bafy"], 2)
        assert result == {"bafy": "promotion_requested"}
        assert len(_FakeAsyncClient.calls) == 1
        assert _FakeAsyncClient.calls[0][2]["params"] == {
            "replication-min": 1,
            "replication-max": 2,
        }

    @pytest.mark.asyncio
    async def test_store_accepts_cluster_rest_lowercase_cid_response(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): _FakeResponse(
                200,
                [{"name": "probe.txt", "cid": "bafk-real", "size": 16, "allocations": ["peer-a"]}],
            ),
        }

        info = await backend().store(b"data")

        assert info.cid == "bafk-real"
        assert info.size == 16

    def test_replica_count_includes_only_pinned_states(self):
        instance = backend()
        total, statuses = instance._pinned_distribution(
            {
                "peer_map": {
                    "one": {"peername": "site-a-storage-1", "status": "pinned"},
                    "two": {"peername": "site-a-storage-2", "status": "pinning"},
                    "three": {"peername": "site-b-storage-1", "status": "pin_error"},
                }
            }
        )
        assert total == 1
        assert statuses == {"pinned", "pinning", "pin_error"}

    @pytest.mark.asyncio
    async def test_store_round_robin_rotates_the_first_cluster_api(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): _FakeResponse(
                200, {"Hash": "bafy-a", "Size": "1"}
            ),
            ("POST", "http://cluster-b/add"): _FakeResponse(
                200, {"Hash": "bafy-b", "Size": "1"}
            ),
        }
        instance = backend()
        await instance.store(b"a")
        await instance.store(b"b")
        add_urls = [
            url
            for method, url, _ in _FakeAsyncClient.calls
            if method == "POST" and url.endswith("/add")
        ]
        assert add_urls == ["http://cluster-a/add", "http://cluster-b/add"]

    @pytest.mark.asyncio
    async def test_failed_cluster_api_cools_down_then_rejoins_round_robin(
        self, monkeypatch
    ):
        now = [100.0]
        monkeypatch.setattr("app.backends.ipfs_cluster.monotonic", lambda: now[0])
        request = httpx.Request("POST", "http://cluster-a/add")
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): [
                httpx.ConnectError("down", request=request),
                _FakeResponse(200, {"Hash": "bafy-c", "Size": "1"}),
            ],
            ("POST", "http://cluster-b/add"): [
                _FakeResponse(200, {"Hash": "bafy-a", "Size": "1"}),
                _FakeResponse(200, {"Hash": "bafy-b", "Size": "1"}),
            ],
        }
        instance = backend(endpoint_cooldown_seconds=30)

        await instance.store(b"a")
        await instance.store(b"b")
        now[0] += 31
        await instance.store(b"c")

        add_urls = [
            url for method, url, _ in _FakeAsyncClient.calls if url.endswith("/add")
        ]
        assert add_urls == [
            "http://cluster-a/add",
            "http://cluster-b/add",
            "http://cluster-b/add",
            "http://cluster-a/add",
        ]

    @pytest.mark.asyncio
    async def test_reuses_and_closes_one_http_client(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/cat"): _FakeResponse(
                200, content=b"payload"
            ),
            ("POST", "http://ipfs-b/api/v0/cat"): _FakeResponse(
                200, content=b"payload"
            ),
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
            ("POST", "http://ipfs-a/api/v0/cat"): httpx.ConnectError(
                "down", request=request
            ),
            ("POST", "http://ipfs-b/api/v0/cat"): _FakeResponse(
                200, content=b"payload"
            ),
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
    async def test_write_health_requires_kubo_and_one_cluster_peer(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(200, {"ID": "kubo"}),
            ("GET", "http://cluster-a/peers"): _FakeResponse(
                200,
                [
                    {"id": "peer-a", "peername": "site-a-storage-1"},
                    {"id": "peer-b", "peername": "site-b-storage-1"},
                ],
            ),
        }
        instance = backend()

        assert await instance.write_health_check()
        assert instance.last_health["available_cluster_peers"] == 2

    @pytest.mark.asyncio
    async def test_write_health_fails_over_from_slow_cluster_peer(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(200, {"ID": "kubo"}),
            ("GET", "http://cluster-a/peers"): httpx.ReadTimeout("slow peer"),
            ("GET", "http://cluster-b/peers"): _FakeResponse(
                200, [{"id": "peer-b", "peername": "storage-b"}]
            ),
        }
        instance = backend()

        assert await instance.write_health_check()
        cluster_calls = [
            call for call in _FakeAsyncClient.calls if call[1].endswith("/peers")
        ]
        assert [call[1] for call in cluster_calls] == [
            "http://cluster-a/peers",
            "http://cluster-b/peers",
        ]
        assert all(call[2]["timeout"] <= 2.0 for call in cluster_calls)

    @pytest.mark.asyncio
    async def test_write_health_accepts_peers_without_site_labels(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://ipfs-a/api/v0/id"): _FakeResponse(200),
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
        assert instance.last_health["available_cluster_peers"] == 2

    @pytest.mark.asyncio
    async def test_store_accepts_cluster_cid_json_object(self):
        _FakeAsyncClient.routes = {
            ("POST", "http://cluster-a/add"): _FakeResponse(
                200, {"cid": {"/": "bafy-cluster-cid"}, "size": 7}
            ),
        }
        info = await backend().store(b"content")
        assert info.cid == "bafy-cluster-cid"
        assert info.size == 7
