"""
Abstract base class for storage backends.

All storage implementations must implement this interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ContentInfo:
    """Information about stored content."""

    cid: str
    size: int
    content_type: str


@dataclass
class PinStatus:
    """Pin/replication status for a CID."""

    cid: str
    pinned: bool
    replicas: int
    status: str  # "pinned", "pinning", "unpinned", "error"


class StorageBackend(ABC):
    """Abstract interface for content storage backends."""

    @abstractmethod
    async def store(self, content: bytes, content_type: str) -> ContentInfo:
        """
        Store content and return content information including CID.

        Args:
            content: Raw bytes to store
            content_type: MIME type (e.g., "application/json", "text/xml")

        Returns:
            ContentInfo with CID, size, and content_type

        Raises:
            StorageError: If storage operation fails
        """
        pass

    @abstractmethod
    async def retrieve(self, cid: str) -> tuple[bytes, str]:
        """
        Retrieve content by CID.

        Args:
            cid: Content identifier

        Returns:
            Tuple of (content_bytes, content_type)

        Raises:
            ContentNotFoundError: If CID not found
            StorageError: If retrieval fails
        """
        pass

    @abstractmethod
    async def status(self, cid: str) -> PinStatus:
        """
        Get pin/replication status for a CID.

        Args:
            cid: Content identifier

        Returns:
            PinStatus with replication info

        Raises:
            ContentNotFoundError: If CID not found
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if storage backend is healthy and accessible.

        Returns:
            True if healthy, False otherwise
        """
        pass


class StorageError(Exception):
    """Base exception for storage errors."""

    pass


class ContentNotFoundError(StorageError):
    """Content not found for given CID."""

    pass
