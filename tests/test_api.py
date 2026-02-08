"""
Tests for storage API endpoints.
"""

import json

import pytest
from fastapi.testclient import TestClient


class TestStoreEndpoint:
    """Tests for POST /v1/store."""

    def test_store_json_content(self, client: TestClient):
        """Store JSON content and get CID."""
        content = {"title": "Test Document", "author": "dARK"}

        response = client.post(
            "/v1/store",
            content=json.dumps(content),
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "cid" in data
        assert data["size"] > 0
        assert data["content_type"] == "application/json"

    def test_store_xml_content(self, client: TestClient):
        """Store XML content."""
        content = '<?xml version="1.0"?><doc><title>Test</title></doc>'

        response = client.post(
            "/v1/store",
            content=content,
            headers={"Content-Type": "text/xml"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "cid" in data
        assert data["content_type"] == "text/xml"

    def test_store_plain_text(self, client: TestClient):
        """Store plain text content."""
        content = "Hello, dARK world!"

        response = client.post(
            "/v1/store",
            content=content,
            headers={"Content-Type": "text/plain"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "cid" in data

    def test_store_empty_body_fails(self, client: TestClient):
        """Empty body should return 400."""
        response = client.post(
            "/v1/store",
            content="",
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 400

    def test_store_same_content_same_cid(self, client: TestClient):
        """Same content should produce same CID (content-addressable)."""
        content = '{"test": "reproducible"}'

        response1 = client.post(
            "/v1/store",
            content=content,
            headers={"Content-Type": "application/json"},
        )
        response2 = client.post(
            "/v1/store",
            content=content,
            headers={"Content-Type": "application/json"},
        )

        assert response1.json()["cid"] == response2.json()["cid"]


class TestRetrieveEndpoint:
    """Tests for GET /v1/retrieve/{cid}."""

    def test_retrieve_stored_content(self, client: TestClient):
        """Retrieve content by CID."""
        original = {"data": "test payload"}

        # Store
        store_response = client.post(
            "/v1/store",
            content=json.dumps(original),
            headers={"Content-Type": "application/json"},
        )
        cid = store_response.json()["cid"]

        # Retrieve
        response = client.get(f"/v1/retrieve/{cid}")

        assert response.status_code == 200
        assert response.json() == original

    def test_retrieve_xml_content(self, client: TestClient):
        """Retrieve XML content."""
        xml_content = "<root><item>value</item></root>"

        store_response = client.post(
            "/v1/store",
            content=xml_content,
            headers={"Content-Type": "text/xml"},
        )
        cid = store_response.json()["cid"]

        response = client.get(f"/v1/retrieve/{cid}")

        assert response.status_code == 200
        assert response.text == xml_content

    def test_retrieve_not_found(self, client: TestClient):
        """Non-existent CID returns 404."""
        response = client.get("/v1/retrieve/nonexistent123")

        assert response.status_code == 404


class TestStatusEndpoint:
    """Tests for GET /v1/status/{cid}."""

    def test_status_pinned_content(self, client: TestClient):
        """Get status of pinned content."""
        # Store content first
        store_response = client.post(
            "/v1/store",
            content="test content",
            headers={"Content-Type": "text/plain"},
        )
        cid = store_response.json()["cid"]

        # Check status
        response = client.get(f"/v1/status/{cid}")

        assert response.status_code == 200
        data = response.json()
        assert data["cid"] == cid
        assert data["pinned"] is True
        assert data["status"] == "pinned"
        assert data["replicas"] >= 1

    def test_status_not_found(self, client: TestClient):
        """Status for unknown CID returns 404."""
        response = client.get("/v1/status/unknowncid123")

        assert response.status_code == 404


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_check(self, client: TestClient):
        """Health endpoint returns service status."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["backend"] == "filesystem"
        assert data["backend_healthy"] is True
        assert "timestamp" in data
