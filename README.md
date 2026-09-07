# dark-store-api

Storage boundary for dARK. Minter and Resolver use Store API without knowing
the global IPFS topology. Store API talks to a generated local failover pool;
IPFS Cluster remains one global CRDT cluster, so Store APIs never call one
another.

## API behavior

| Method | Endpoint | Meaning |
| --- | --- | --- |
| `POST` | `/v1/store` | Add through Cluster REST and return the CID immediately |
| `GET` | `/v1/retrieve/{cid}` | Read through either local Kubo |
| `GET` | `/v1/status/{cid}` | Observe global pin status |
| `GET` | `/health/live` | Process liveness only |
| `GET` | `/health/read` | At least one local Kubo is usable |
| `GET` | `/health/write` | At least one local write path and one known Cluster peer are usable |
| `GET` | `/health` | Alias of write readiness |

A successful write returns the accepted CID; it does not imply a pin has been
observed yet:

```json
{
  "cid": "bafy...",
  "size": 14520,
  "replication": null
}
```

Store API returns the CID when Cluster accepts the local `add`; it does not
wait for the first observed pin. The replication worker later observes and
repairs replicas, and Store API never unpins automatically.

## Configuration

The deployer resolves the selected access group from the shared production
topology and generates `.storage-endpoints.json`, mounted read-only at
`/config/storage-endpoints.json`. Store API receives no site, peer map, tag or
full topology: only its Kubo and Cluster REST failover endpoints.

| Variable | Description |
| --- | --- |
| `STORAGE_ENDPOINTS_FILE` | Mounted generated endpoint document inside the container |
| `IPFS_HEALTH_CACHE_TTL_SECONDS` | Write-readiness cache TTL |

Kubo and Cluster REST endpoints use round-robin selection. A
timeout, refused connection, `429` or `5xx` places an endpoint in a 30-second
cooldown; it rejoins automatically. Store returns after Cluster accepts the CID;
the replication worker observes and repairs pins asynchronously.
`GET /v1/status/{cid}` reports the observed total pinned copies. Publication and
payload-retention policy are owned by the minter, not by Store API.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

The filesystem backend exists only for isolated tests and does not represent a
supported deployment architecture.
