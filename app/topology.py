"""Read the generated, minimal Store API endpoint document."""

from __future__ import annotations

import json
from pathlib import Path


class StorageTopologyError(ValueError):
    """Raised when the generated Store API endpoint document is invalid."""


def load_endpoints(topology_file: Path) -> dict[str, list[str]]:
    """Return a failover pool, without interpreting production topology."""
    try:
        document = json.loads(topology_file.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StorageTopologyError(f"storage endpoint file not found: {topology_file}") from exc
    except json.JSONDecodeError as exc:
        raise StorageTopologyError(f"invalid storage endpoint JSON: {exc}") from exc
    if not isinstance(document, dict) or document.get("version") != 1:
        raise StorageTopologyError("storage endpoint document must use version 1")
    nodes = document.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise StorageTopologyError("storage endpoint document must contain nodes")
    endpoints = {"ipfs": [], "cluster": []}
    seen: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise StorageTopologyError(f"nodes[{index}] must be an object")
        node_id = node.get("id")
        ipfs_url = node.get("ipfs_api_url")
        cluster_url = node.get("cluster_api_url")
        if not isinstance(node_id, str) or not node_id or node_id in seen:
            raise StorageTopologyError(f"nodes[{index}] has an invalid or duplicate id")
        if not isinstance(ipfs_url, str) or not ipfs_url.startswith(("http://", "https://")):
            raise StorageTopologyError(f"nodes[{index}].ipfs_api_url must be an HTTP URL")
        if not isinstance(cluster_url, str) or not cluster_url.startswith(("http://", "https://")):
            raise StorageTopologyError(f"nodes[{index}].cluster_api_url must be an HTTP URL")
        seen.add(node_id)
        endpoints["ipfs"].append(ipfs_url.rstrip("/"))
        endpoints["cluster"].append(cluster_url.rstrip("/"))
    return endpoints
