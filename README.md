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
| `POST` | `/v1/status/batch` | Observe hasta 200 CIDs con concurrencia acotada |
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
`GET /v1/status/{cid}` reports confirmed copies and separates `queued`,
`pinning`, and `error` assignments. `POST /v1/status/batch` returns the same
contract for a bounded list and is used by the replication worker to observe
L1 y L2 together. Publication and payload-retention policy are owned by the
minter, not by Store API.

`total_replicas` counts only confirmed `pinned` peers; queued and pinning are
normal asynchronous progress, while `error` is a real Cluster error.
`assigned_replicas` counts all peer assignments. Store API reports health as
failed when multiple visible Cluster peers advertise the same Kubo peer
identity, because that would make HA replica counts misleading.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

The filesystem backend exists only for isolated tests and does not represent a
supported deployment architecture.

## Pinning contract used by the minter

`POST /v1/store` is an acceptance operation. Cluster returns a CID as soon as
the local `add` request is accepted; Store API does not wait for a peer to
report `pinned`. The returned CID must therefore be treated as `stored`, not
as proof of availability.

The minter's replication worker later calls `/v1/status/{cid}` or the bounded
`/v1/status/batch` endpoint. Only confirmed `pinned` assignments count toward
replication. Chain publication is enabled only after both L1 and L2 satisfy the
configured publish threshold; Store API itself never decides publication or
payload purge.

The initial add explicitly requests `replication-min=1` and
`replication-max=1`. The internal `POST /v1/replication/ensure` operation can
later raise an existing pin to the configured target without uploading the
payload again. It is intended for the minter's idle durability phase.
