# dark-store-api

Site-local storage boundary for dARK. Each application site runs one Store API;
Minter and Resolver use it without knowing the global IPFS topology.

Store API talks to both Kubo/Cluster pairs in its own site. IPFS Cluster is one
global CRDT cluster, so Store APIs never call one another.

## API behavior

| Method | Endpoint | Meaning |
| --- | --- | --- |
| `POST` | `/v1/store` | Add through a local Cluster Proxy and confirm one pinned peer |
| `GET` | `/v1/retrieve/{cid}` | Read through either local Kubo |
| `GET` | `/v1/status/{cid}` | Observe global pin status |
| `GET` | `/health/live` | Process liveness only |
| `GET` | `/health/read` | At least one local Kubo is usable |
| `GET` | `/health/write` | At least one local write path and one known Cluster peer are usable |
| `GET` | `/health` | Alias of write readiness |

A successful write includes the live replication snapshot observed after the
first pin:

```json
{
  "cid": "bafy...",
  "size": 14520,
  "replication": {
    "total_replicas": 1,
    "local_replicas": 1,
    "remote_replicas": 0,
    "sites": {"site-a": 1},
    "purge_target_met": false,
    "checked_at": "2026-08-21T12:00:00Z"
  }
}
```

If the first pin is not observed before the timeout, the API
returns `503 Service Unavailable` with `Retry-After: 5`. The partial immutable
pin is left for Cluster recovery; Store API never unpins automatically.

## Configuration

The deployer generates `.env.integration` from the shared topology.

| Variable | Description |
| --- | --- |
| `IPFS_API_URLS_JSON` | Ordered local Kubo endpoints |
| `IPFS_CLUSTER_API_URLS_JSON` | Ordered local Cluster REST endpoints |
| `IPFS_CLUSTER_PROXY_API_URLS_JSON` | Ordered local Cluster Proxy endpoints |
| `IPFS_CLUSTER_PEER_SITES_JSON` | Cluster peer-name to site mapping |
| `IPFS_CLUSTER_LOCAL_SITE_ID` | Site whose two endpoints are local to this Store API |
| `IPFS_REPLICATION_CONFIRM_TIMEOUT_SECONDS` | Maximum durability wait |
| `IPFS_HEALTH_CACHE_TTL_SECONDS` | Write-readiness cache TTL |

Kubo, Cluster REST and Cluster Proxy endpoints use round-robin selection. A
timeout, refused connection, `429` or `5xx` places an endpoint in a 30-second
cooldown; it rejoins automatically. Store confirms the first `PINNED` peer.
`GET /v1/status/{cid}` reports total, local, remote and per-site copies plus the
topology-derived `purge_target_met` decision.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

The filesystem backend exists only for isolated tests and does not represent a
supported deployment architecture.
