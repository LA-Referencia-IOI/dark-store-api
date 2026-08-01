# dark-store-api

Site-local storage boundary for dARK. Each application site runs one Store API;
Minter and Resolver use it without knowing the global IPFS topology.

Store API talks to both Kubo/Cluster pairs in its own site. IPFS Cluster is one
global CRDT cluster, so Store APIs never call one another.

## API behavior

| Method | Endpoint | Meaning |
| --- | --- | --- |
| `POST` | `/v1/store` | Add through a local Cluster Proxy and wait for durable quorum |
| `GET` | `/v1/retrieve/{cid}` | Read through either local Kubo |
| `GET` | `/v1/status/{cid}` | Observe global pin status |
| `GET` | `/health/live` | Process liveness only |
| `GET` | `/health/read` | At least one local Kubo is usable |
| `GET` | `/health/write` | Local write path plus peer/site quorum is usable |
| `GET` | `/health` | Alias of write readiness |

A successful write includes the durability evidence observed before returning:

```json
{
  "cid": "bafy...",
  "size": 14520,
  "replication": {
    "status": "durable",
    "pinned_peers": 3,
    "pinned_sites": 2,
    "target_peers": 4
  }
}
```

If the configured peer/site quorum is not reached before the timeout, the API
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
| `IPFS_CLUSTER_EXPECTED_PEERS` | Total peers in the topology |
| `IPFS_CLUSTER_WRITE_MIN_PEERS` | Actual pinned/healthy peers required |
| `IPFS_CLUSTER_WRITE_MIN_SITES` | Actual pinned/healthy sites required |
| `IPFS_REPLICATION_CONFIRM_TIMEOUT_SECONDS` | Maximum durability wait |
| `IPFS_REPLICATION_CONFIRM_INTERVAL_SECONDS` | Status poll interval |
| `IPFS_HEALTH_CACHE_TTL_SECONDS` | Write-readiness cache TTL |

Endpoint failover retries a complete request on the second local endpoint.
Reads do not create pins; persistent allocation and repair remain Cluster's job.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

The filesystem backend exists only for isolated tests and does not represent a
supported deployment architecture.
