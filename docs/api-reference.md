# Store API reference

Base URL: service root (port `8003`). OpenAPI: `/docs`, `/redoc` and
`/openapi.json` (FastAPI defaults). There is **no application authentication**:
access control relies on private networking, a proxy and `STORE_API_MODE`.

## Endpoints

### `POST /v1/store`

Body: raw bytes (any content type). Empty body → `400`.
`200`: `{"cid": "bafy...", "size": 14520, "replication": null}`.
`403` in `read_only` mode; `500` on a storage failure (all Cluster endpoints
failed). The CID means *accepted*, not *pinned*.

### `GET /v1/retrieve/{cid}`

Returns the content as `application/octet-stream` read through the local Kubo
pool. `404` `Content not found: <cid>`; `500` on backend failure.

### `GET /v1/status/{cid}`

```json
{
  "cid": "bafy...",
  "status": "pinned",
  "replication": {
    "total_replicas": 2, "queued_replicas": 0, "pinning_replicas": 0,
    "error_replicas": 0, "assigned_replicas": 2,
    "checked_at": "2026-01-01T00:00:00Z"
  },
  "error": null
}
```

`status` ∈ `pinned`, `pinning`, `queued`, `unpinned`, `error`, `unknown`. `404`
`Pin not found`; `500` on backend failure.

### `POST /v1/status/batch`

Body `{"cids": ["...", ...]}` with 1–200 CIDs. Response
`{"statuses": [<status object>, ...]}`. Observation is partial: a CID that times out
or fails carries its own `error` while the rest of the page is still returned.

### `POST /v1/replication/ensure`

```json
{ "cids": ["bafy..."], "target_replicas": 2, "assigned_replicas": {"bafy...": 1} }
```

1–200 CIDs, `target_replicas` between 1 and 200, optional `assigned_replicas`
(counts from the previous status batch). Response
`{"results": {"<cid>": "promotion_requested" | "already_allocated" | "promotion_failed"}}`.
`422` when `target_replicas` exceeds `REPLICATION_TARGET_REPLICAS`; `403` in
`read_only` mode; `500` on backend failure. It changes the allocation only and does
not re-upload or wait for new pins.

## Health

| Path | Semantics | Failure |
|---|---|---|
| `GET /health/live` | Process liveness only | — |
| `GET /health/read` | At least one local Kubo is usable | `503` |
| `GET /health/write` | A local write path and at least one known Cluster peer (`min_cluster_peers: 1`) are usable; also fails on duplicate Kubo peer identities | `503` (always in `read_only`) |
| `GET /health` | Write readiness; in `read_only` mode, read readiness | `503` |

`HealthResponse` fields: `status` (`healthy`/`unhealthy`), `backend`,
`backend_healthy`, `dimension` (`read`/`write`), `min_cluster_peers`,
`available_cluster_peers`, `duplicate_ipfs_peer_ids`, `error`, `cached`,
`checked_at`, `cache_ttl_seconds`, `timestamp`.
