import json

import pytest

from app.topology import StorageTopologyError, load_endpoints


def test_load_endpoints_uses_generated_failover_pool(tmp_path):
    document = tmp_path / "storage-endpoints.json"
    document.write_text(json.dumps({"version": 1, "nodes": [
        {"id": "storage-a", "ipfs_api_url": "http://10.200.1.11:5001", "cluster_api_url": "http://10.200.1.11:9094"},
        {"id": "storage-b", "ipfs_api_url": "http://10.200.1.12:5001", "cluster_api_url": "http://10.200.1.12:9094"},
    ]}))
    endpoints = load_endpoints(document)
    assert endpoints["ipfs"] == ["http://10.200.1.11:5001", "http://10.200.1.12:5001"]
    assert endpoints["cluster"] == ["http://10.200.1.11:9094", "http://10.200.1.12:9094"]


def test_load_endpoints_rejects_legacy_or_duplicate_nodes(tmp_path):
    document = tmp_path / "storage-endpoints.json"
    document.write_text(json.dumps({"version": 2, "sites": []}))
    with pytest.raises(StorageTopologyError, match="version 1"):
        load_endpoints(document)
    document.write_text(json.dumps({"version": 1, "nodes": [
        {"id": "a", "ipfs_api_url": "http://a", "cluster_api_url": "http://a"},
        {"id": "a", "ipfs_api_url": "http://b", "cluster_api_url": "http://b"},
    ]}))
    with pytest.raises(StorageTopologyError, match="duplicate"):
        load_endpoints(document)
