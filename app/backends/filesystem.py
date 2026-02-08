"""
Filesystem-based storage backend for development and testing.

Uses MD5 hash as CID (not a real IPFS CID, but useful for local dev).
"""

import hashlib
import json
import logging
from pathlib import Path

from .base import (
    StorageBackend,
    ContentInfo,
    PinStatus,
    StorageError,
    ContentNotFoundError,
)


logger = logging.getLogger(__name__)


class FileSystemBackend(StorageBackend):
    """
    Filesystem implementation of storage backend.

    Files are stored as {md5_hash}.dat with a .meta sidecar for content-type.
    Uses MD5 hash as a pseudo-CID for content-addressable storage.
    """

    def __init__(self, storage_path: str):
        """
        Initialize filesystem storage.

        Args:
            storage_path: Directory path for storing content files
        """
        self.storage_path = Path(storage_path)
        self._ensure_storage_directory()

    def _ensure_storage_directory(self) -> None:
        """Create storage directory if it doesn't exist."""
        try:
            self.storage_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage directory ready: {self.storage_path}")
        except Exception as e:
            raise StorageError(f"Failed to create storage directory: {e}")

    def _calculate_cid(self, content: bytes) -> str:
        """Calculate MD5 hash of content as pseudo-CID."""
        return hashlib.md5(content).hexdigest()

    def _sanitize_cid(self, cid: str) -> str:
        """Sanitize CID to prevent directory traversal."""
        safe_cid = Path(cid).name
        if safe_cid != cid:
            raise StorageError(f"Invalid CID format: {cid}")
        return safe_cid

    def _get_content_path(self, cid: str) -> Path:
        """Get content file path for a given CID."""
        safe_cid = self._sanitize_cid(cid)
        return self.storage_path / f"{safe_cid}.dat"

    def _get_meta_path(self, cid: str) -> Path:
        """Get metadata sidecar file path."""
        safe_cid = self._sanitize_cid(cid)
        return self.storage_path / f"{safe_cid}.meta"

    async def store(self, content: bytes, content_type: str) -> ContentInfo:
        """Store content and return ContentInfo with MD5-based CID."""
        try:
            cid = self._calculate_cid(content)
            content_path = self._get_content_path(cid)
            meta_path = self._get_meta_path(cid)

            # Write content atomically using temp file + rename
            temp_content = content_path.with_suffix(".tmp")
            temp_content.write_bytes(content)
            temp_content.replace(content_path)

            # Write metadata
            meta_data = {"content_type": content_type, "size": len(content)}
            temp_meta = meta_path.with_suffix(".tmp")
            temp_meta.write_text(json.dumps(meta_data), encoding="utf-8")
            temp_meta.replace(meta_path)

            logger.info(f"Stored content with CID: {cid} (type: {content_type})")

            return ContentInfo(cid=cid, size=len(content), content_type=content_type)

        except Exception as e:
            logger.error(f"Failed to store content: {e}")
            raise StorageError(f"Storage failed: {e}")

    async def retrieve(self, cid: str) -> tuple[bytes, str]:
        """Retrieve content by CID."""
        try:
            content_path = self._get_content_path(cid)
            meta_path = self._get_meta_path(cid)

            if not content_path.exists():
                raise ContentNotFoundError(f"Content not found for CID: {cid}")

            content = content_path.read_bytes()

            # Get content type from metadata
            content_type = "application/octet-stream"
            if meta_path.exists():
                meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
                content_type = meta_data.get("content_type", content_type)

            logger.debug(f"Retrieved content for CID: {cid}")
            return content, content_type

        except ContentNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to retrieve content for CID {cid}: {e}")
            raise StorageError(f"Retrieval failed: {e}")

    async def status(self, cid: str) -> PinStatus:
        """Get pin status - always 'pinned' with 1 replica for filesystem."""
        content_path = self._get_content_path(cid)

        if not content_path.exists():
            raise ContentNotFoundError(f"Content not found for CID: {cid}")

        return PinStatus(
            cid=cid,
            pinned=True,
            replicas=1,  # Filesystem has no replication
            status="pinned",
        )

    async def health_check(self) -> bool:
        """Check if storage directory is accessible."""
        try:
            if not self.storage_path.exists():
                logger.error(f"Storage directory does not exist: {self.storage_path}")
                return False

            # Check directory is writable
            test_file = self.storage_path / ".health_check"
            test_file.touch()
            test_file.unlink()

            return True

        except Exception as e:
            logger.error(f"Storage health check failed: {e}")
            return False
