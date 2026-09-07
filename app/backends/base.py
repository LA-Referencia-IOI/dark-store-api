"""
Abstract base class for storage backends.

All storage implementations must implement this interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ReplicationInfo:
    """Observed replication and the topology-derived purge decision."""

    total_replicas: int
    checked_at: datetime | None = None


@dataclass
class ContentInfo:
    """Information about stored content."""

    cid: str
    size: int
    replication: ReplicationInfo | None = None


@dataclass
class PinStatus:
    """Pin status and current replication snapshot for a CID."""

    cid: str
    status: str  # "pinned", "pinning", "unpinned", "error"
    replication: ReplicationInfo


class StorageBackend(ABC):
    """Abstract interface for content storage backends."""

    @abstractmethod
    async def store(self, content: bytes) -> ContentInfo:
        """
        Store raw content and return content information including CID.

        Args:
            content: Raw bytes to store

        Returns:
            ContentInfo with CID and size

        Raises:
            StorageError: If storage operation fails
        """
        pass

    @abstractmethod
    async def retrieve(self, cid: str) -> bytes:
        """
        Retrieve content by CID.

        Args:
            cid: Content identifier

        Returns:
            Raw content bytes

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
    async def health_check(self, refresh: bool = False) -> bool:
        """
        Check if storage backend is healthy and accessible.

        Args:
            refresh: Force a fresh backend check when a backend supports caching.

        Returns:
            True if healthy, False otherwise
        """
        pass


class StorageError(Exception):
    """Base exception for storage errors."""

    pass


class ReplicationQuorumError(StorageError):
    """A pin exists but did not reach the configured durable write quorum."""

    pass


class ContentNotFoundError(StorageError):
    """Content not found for given CID."""

    pass
