"""
Filesystem-based storage backend for development and testing.

Uses MD5 hash as CID (not a real IPFS CID, but useful for local dev).
"""

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from .base import (
    StorageBackend,
    ContentInfo,
    PinStatus,
    ReplicationInfo,
    StorageError,
    ContentNotFoundError,
)


logger = logging.getLogger(__name__)


class FileSystemBackend(StorageBackend):
    """
    Filesystem implementation of storage backend.

    Files are stored as raw blobs named by their MD5 pseudo-CID.
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
        return self.storage_path / safe_cid

    async def store(self, content: bytes) -> ContentInfo:
        """Store content and return ContentInfo with MD5-based CID."""
        try:
            cid = self._calculate_cid(content)
            content_path = self._get_content_path(cid)

            # Write content atomically using temp file + rename
            temp_content = content_path.with_suffix(".tmp")
            temp_content.write_bytes(content)
            temp_content.replace(content_path)

            logger.info(f"Stored raw content with CID: {cid}")

            return ContentInfo(
                cid=cid,
                size=len(content),
                replication=ReplicationInfo(
                    total_replicas=1,
                    checked_at=datetime.now(timezone.utc),
                ),
            )

        except Exception as e:
            logger.error(f"Failed to store content: {e}")
            raise StorageError(f"Storage failed: {e}")

    async def retrieve(self, cid: str) -> bytes:
        """Retrieve content by CID."""
        try:
            content_path = self._get_content_path(cid)

            if not content_path.exists():
                raise ContentNotFoundError(f"Content not found for CID: {cid}")

            logger.debug(f"Retrieved content for CID: {cid}")
            return content_path.read_bytes()

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
            status="pinned",
            replication=ReplicationInfo(
                total_replicas=1,
                checked_at=datetime.now(timezone.utc),
            ),
        )

    async def ensure_replication(self, cids: list[str], target_replicas: int) -> dict[str, str]:
        """The test filesystem is already durable at its single replica."""
        return {cid: "already_durable" for cid in cids}

    async def health_check(self, refresh: bool = False) -> bool:
        """Check if storage directory is accessible."""
        del refresh
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
