# dark-store-api

Stateless IPFS storage API for the dARK project. Provides transparent content-addressable storage over IPFS Cluster with a simple REST interface.

## Overview

`dark-store-api` abstracts IPFS storage behind a clean HTTP API. Store any content (JSON, XML, text, binary) and get back a Content Identifier (CID). Retrieve content by CID. No database, no state—the CID is the only identifier.

### Key Features

- **Content-addressable storage**: Same content always produces the same CID
- **Format agnostic**: Supports JSON, XML, plain text, binary data
- **Pluggable backends**: IPFS Cluster (production) or filesystem (development)
- **100% stateless**: No database required
- **Simple REST API**: Store, retrieve, check status

## Quick Start

```bash
# Navigate to module
cd /Users/lmatas/source/dark/dark-store-api

# Create .env from template
cp .env.example .env

# Install in virtual environment
source /Users/lmatas/source/dark/.venv/bin/activate
pip install -e ".[dev]"

# Run with filesystem backend (no IPFS needed)
STORAGE_BACKEND=filesystem uvicorn app.main:app --port 8002 --reload
```

### With IPFS Cluster

```bash
# Start IPFS cluster first
cd /Users/lmatas/source/dark/dark-ipfs
make up

# Then start the API
cd /Users/lmatas/source/dark/dark-store-api
uvicorn app.main:app --port 8002
```

## API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/store` | Store content and return CID |
| `GET` | `/v1/retrieve/{cid}` | Retrieve content by CID |
| `GET` | `/v1/status/{cid}` | Get pin/replication status |
| `GET` | `/health` | Service health check |

### Store Content

```bash
curl -X POST http://localhost:8002/v1/store \
  -H "Content-Type: application/json" \
  -d '{"title":"My Document","author":"dARK"}'
```

**Response:**
```json
{
  "cid": "bafkreidxvj2rtmmscbwzuss4yx6rpgp434rw2hfufhp4a4t4enb3hi4yd4",
  "size": 42,
  "content_type": "application/json"
}
```

### Retrieve Content

```bash
curl http://localhost:8002/v1/retrieve/bafkreidxvj2rtmmscbwzuss...
```

Returns the original content with the stored `Content-Type` header.

### Check Status

```bash
curl http://localhost:8002/v1/status/bafkreidxvj2rtmmscbwzuss...
```

**Response:**
```json
{
  "cid": "bafkreidxvj2rtmmscbwzuss...",
  "pinned": true,
  "replicas": 3,
  "status": "pinned"
}
```

### Health Check

```bash
curl http://localhost:8002/health
```

**Response:**
```json
{
  "status": "healthy",
  "backend": "ipfs_cluster",
  "backend_healthy": true,
  "timestamp": "2026-02-08T15:00:00Z"
}
```

## Configuration

Configuration is managed via environment variables. Copy `.env.example` to `.env` and adjust as needed.

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BACKEND` | `ipfs_cluster` | Backend type: `ipfs_cluster` or `filesystem` |
| `IPFS_API_URL` | `http://localhost:5001` | IPFS node API endpoint |
| `IPFS_CLUSTER_API_URL` | `http://localhost:9094` | IPFS Cluster REST API endpoint |
| `FILESYSTEM_STORAGE_PATH` | `./storage` | Path for filesystem backend |
| `STORE_API_HOST` | `0.0.0.0` | API server bind address |
| `STORE_API_PORT` | `8002` | API server port |
| `LOG_LEVEL` | `INFO` | Logging level |

### Unified Configuration

Variables are also available in the root `/Users/lmatas/source/dark/.env` file for unified project configuration.

## Architecture

```
dark-store-api/
├── app/
│   ├── api/
│   │   └── store.py            # REST endpoints
│   ├── backends/
│   │   ├── base.py             # Abstract StorageBackend interface
│   │   ├── filesystem.py       # Development/test backend (MD5 as CID)
│   │   └── ipfs_cluster.py     # Production backend (real IPFS CIDs)
│   ├── models/
│   │   └── responses.py        # Pydantic response models
│   ├── config.py               # Environment configuration
│   ├── dependencies.py         # Dependency injection
│   └── main.py                 # FastAPI application
├── notebooks/
│   └── test_store_api.ipynb    # Interactive test notebook
├── tests/
│   ├── test_api.py             # API endpoint tests
│   └── test_backends.py        # Backend unit tests
└── pyproject.toml
```

### Storage Backends

#### IPFS Cluster Backend (Production)

Uses the IPFS HTTP API for content operations and IPFS Cluster REST API for pin management:

- **Store**: `POST /api/v0/add` → returns real IPFS CID (e.g., `bafkreid...`)
- **Retrieve**: `POST /api/v0/cat` → returns content bytes
- **Status**: `GET /pins/{cid}` → returns replication info

#### Filesystem Backend (Development)

Stores content locally using MD5 hash as pseudo-CID:

- Files stored as `{md5}.dat` with `.meta` sidecar
- Single replica, always "pinned"
- No external dependencies

## Testing

### Unit Tests

```bash
# Run with filesystem backend (no IPFS required)
cd /Users/lmatas/source/dark/dark-store-api
source /Users/lmatas/source/dark/.venv/bin/activate
pytest tests/ -v
```

### Integration Tests

```bash
# Start IPFS cluster
make -C /Users/lmatas/source/dark/dark-ipfs up

# Run API with cluster backend
STORAGE_BACKEND=ipfs_cluster uvicorn app.main:app --port 8002 &

# Test endpoints
curl -X POST http://localhost:8002/v1/store \
  -H "Content-Type: application/json" \
  -d '{"test":"integration"}'
```

### Interactive Notebook

Open `notebooks/test_store_api.ipynb` for interactive testing with Jupyter.

## Integration with dARK Ecosystem

### Port Allocation

| Service | Port |
|---------|------|
| Admin API | 8000 |
| Minter API | 8001 |
| **Store API** | **8002** |
| IPFS API | 5001 |
| IPFS Gateway | 8080 |
| Cluster REST | 9094 |

### Using from dark-core-minter-api

To use `dark-store-api` as metadata storage backend for the minter:

```python
# Example client implementation
import httpx

class DarkStoreClient:
    def __init__(self, base_url: str = "http://localhost:8002"):
        self.base_url = base_url

    def store(self, content: str, content_type: str = "application/json") -> str:
        response = httpx.post(
            f"{self.base_url}/v1/store",
            content=content.encode(),
            headers={"Content-Type": content_type}
        )
        return response.json()["cid"]

    def retrieve(self, cid: str) -> bytes:
        response = httpx.get(f"{self.base_url}/v1/retrieve/{cid}")
        return response.content
```

## API Documentation

Interactive API documentation is available at:

- **Swagger UI**: http://localhost:8002/docs
- **ReDoc**: http://localhost:8002/redoc

## License

AGPL-3.0-or-later

This module is part of the [dARK project](https://github.com/lareferencia/dark).
