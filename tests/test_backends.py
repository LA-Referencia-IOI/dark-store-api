"""
Tests for storage backends.
"""

import os
import tempfile

import pytest

from app.backends.filesystem import FileSystemBackend
from app.backends.base import ContentNotFoundError


class TestFileSystemBackend:
    """Tests for FileSystemBackend."""

    @pytest.fixture
    def backend(self) -> FileSystemBackend:
        """Create a backend with temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield FileSystemBackend(tmpdir)

    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, backend: FileSystemBackend):
        """Store and retrieve content."""
        content = b'{"test": "data"}'

        info = await backend.store(content)

        assert info.cid is not None
        assert info.size == len(content)

        # Retrieve
        retrieved = await backend.retrieve(info.cid)

        assert retrieved == content

    @pytest.mark.asyncio
    async def test_content_addressable(self, backend: FileSystemBackend):
        """Same content produces same CID."""
        content = b"reproducible content"

        info1 = await backend.store(content)
        info2 = await backend.store(content)

        assert info1.cid == info2.cid

    @pytest.mark.asyncio
    async def test_retrieve_not_found(self, backend: FileSystemBackend):
        """Retrieve non-existent CID raises error."""
        with pytest.raises(ContentNotFoundError):
            await backend.retrieve("nonexistent123")

    @pytest.mark.asyncio
    async def test_status_pinned(self, backend: FileSystemBackend):
        """Status shows pinned for stored content."""
        info = await backend.store(b"test")

        status = await backend.status(info.cid)

        assert status.cid == info.cid
        assert status.pinned is True
        assert status.status == "pinned"
        assert status.replicas == 1  # Filesystem has single replica

    @pytest.mark.asyncio
    async def test_status_not_found(self, backend: FileSystemBackend):
        """Status for unknown CID raises error."""
        with pytest.raises(ContentNotFoundError):
            await backend.status("unknowncid")

    @pytest.mark.asyncio
    async def test_health_check(self, backend: FileSystemBackend):
        """Health check returns True for valid directory."""
        result = await backend.health_check()
        assert result is True

    @pytest.mark.asyncio
    async def test_binary_content(self, backend: FileSystemBackend):
        """Store and retrieve binary content."""
        content = bytes(range(256))  # All byte values

        info = await backend.store(content)
        retrieved = await backend.retrieve(info.cid)

        assert retrieved == content
