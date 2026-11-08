# Store API configuration

Settings come from environment variables (`app/config.py`, pydantic-settings;
`.env.integration` is preferred over `.env` when present). In a deployment the
deployer generates the environment and the endpoint document from the operator
inventory (`storage` and `overrides.settings.store` sections).

| Variable | Default | Meaning |
|---|---|---|
| `STORE_API_HOST` | `0.0.0.0` | Bind address of the `dark-store-api` entry point |
| `STORE_API_PORT` | `8003` | Port (the Dockerfile exposes `8003`) |
| `STORE_API_MODE` | `read_write` | `read_write` or `read_only` (a reader rejects publication and pin-allocation changes with `403`) |
| `STORAGE_BACKEND` | `ipfs_cluster` | `ipfs_cluster` (production) or `filesystem` (tests only) |
| `STORAGE_ENDPOINTS_FILE` | `/config/storage-endpoints.json` | Generated endpoint document ([format](architecture.md#endpoint-document)) |
| `IPFS_HEALTH_CACHE_TTL_SECONDS` | `10` | Write-readiness cache TTL |
| `REPLICATION_TARGET_REPLICAS` | `2` | Highest `target_replicas` accepted by `POST /v1/replication/ensure` (`422` above it) |
| `STORE_ADD_CONCURRENCY` | `4` | Concurrent adds |
| `STORE_STATUS_CONCURRENCY` | `6` | Concurrent status observations |
| `STORE_PROMOTION_CONCURRENCY` | `4` | Concurrent promotions |
| `FILESYSTEM_STORAGE_PATH` | `./storage` | Directory of the test-only filesystem backend |
| `LOG_LEVEL` | `WARNING` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; applies to the app and Uvicorn |

`STORAGE_SITE_ID` appears in `.env.example` but is not read by `app/config.py`
(extra variables are ignored): site selection is resolved by the deployer before
the endpoint document is generated.

The inventory exposes the same tuning as `overrides.settings.store`:
`add_concurrency`, `status_concurrency`, `promotion_concurrency` and
`logging.level`.

## Image and process

`python:3.14-slim`, non-root `appuser`, `pip install .` from the component
directory (build context must contain `dark-store-api/`), `CMD ["dark-store-api"]`
(console script from `pyproject.toml`). Mount the endpoint document read-only at
`/config/storage-endpoints.json`.
