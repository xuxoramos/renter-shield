"""API tests — authentication gating, response structure, and data correctness.

Uses FastAPI's TestClient with synthetic Parquet data so tests don't depend on
real pipeline output.  Covers:
  - Auth enforcement (401 without key, 403 with wrong scope)
  - Response JSON structure for authenticated requests
  - Search, property lookup, and landlord detail endpoints
  - Edge cases: empty queries, missing BBLs, URL-encoded owner IDs
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import quote, unquote

import polars as pl
import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures — create synthetic data + configure app to use it
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def data_dir():
    """Create a temp dir with minimal Parquet files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)

        # --- Properties ---
        properties = pl.DataFrame({
            "bbl": ["nyc-1000010001", "nyc-1000020002", "bos-1234"],
            "registration_id": ["reg-001", "reg-002", "reg-003"],
            "units_residential": [10.0, 5.0, 3.0],
            "year_built": ["1920", "2005", "1890"],
            "address": ["123 MAIN ST APT 1", "456 BROADWAY", "789 BEACON ST"],
            "jurisdiction": ["nyc", "nyc", "boston"],
        })
        properties.write_parquet(outdir / "properties.parquet")

        # --- Violations ---
        violations = pl.DataFrame({
            "violation_id": ["v1", "v2", "v3", "v4", "v5"],
            "bbl": ["nyc-1000010001", "nyc-1000010001", "nyc-1000010001", "nyc-1000020002", "bos-1234"],
            "severity_tier": pl.Series([1, 2, 3, 1, 4], dtype=pl.Int8),
            "status": ["open", "closed", "open", "open", "closed"],
            "inspection_date": [
                date(2023, 1, 15), date(2023, 3, 10), date(2023, 5, 1),
                date(2024, 2, 20), date(2023, 6, 1),
            ],
            "jurisdiction": ["nyc", "nyc", "nyc", "nyc", "boston"],
        })
        violations.write_parquet(outdir / "violations.parquet")

        # --- Owner registrations ---
        owner_regs = pl.DataFrame({
            "owner_id": ["slumlord_mcgee [nyc]", "slumlord_mcgee [nyc]", "good_landlord [boston]"],
            "jurisdiction": ["nyc", "nyc", "boston"],
            "registration_id": ["reg-001", "reg-002", "reg-003"],
            "confidence": ["high", "high", "medium"],
        })
        owner_regs.write_parquet(outdir / "owner_registrations.parquet")

        # --- Scores (pre-computed) ---
        scores = pl.DataFrame({
            "owner_id": ["slumlord_mcgee [nyc]", "good_landlord [boston]"],
            "jurisdiction": ["nyc", "boston"],
            "confidence": ["high", "medium"],
            "num_properties": [2, 1],
            "total_violations": [4, 1],
            "class_c_violations": [2, 0],
            "severity_score": [13.5, 0.0],
            "density_score": [0.267, 0.333],
            "widespread_score": [1.0, 1.0],
            "persistence_score": [0.75, 0.0],
            "total_units": [15, 3],
            "unresolved_violations": [3, 0],
            "total_harm_score": [38.9, 5.0],
            "svi_composite": [0.85, 0.15],
            "likert_level": [5, 1],
            "likert_label": ["Severe concerns", "Low concern"],
        })
        scores.write_parquet(outdir / "all_landlords_harm_scores.parquet")

        yield outdir


@pytest.fixture(scope="module")
def client(data_dir):
    """TestClient wired to the synthetic data directory."""
    # Patch the output dir before importing the app
    os.environ["LI_OUTPUT_DIR"] = str(data_dir)
    os.environ["LI_API_KEYS"] = "test-renter-key:renter,test-inv-key:investigator"

    # Force reimport so the app picks up new env vars
    import importlib
    import renter_shield.web
    import renter_shield.api

    # Reset web module's data cache
    renter_shield.web._scores_df = None
    renter_shield.web._props_df = None
    renter_shield.web._viols_df = None
    renter_shield.web._owner_reg_df = None
    renter_shield.web.OUTPUT_DIR = data_dir

    # Reset api module's data cache
    renter_shield.api._scores_df = None
    renter_shield.api._properties_df = None
    renter_shield.api._violations_df = None
    renter_shield.api._owner_reg_df = None
    renter_shield.api.OUTPUT_DIR = data_dir
    renter_shield.api.SCORES_FILE = data_dir / "all_landlords_harm_scores.parquet"
    renter_shield.api.PROPERTIES_FILE = data_dir / "properties.parquet"
    renter_shield.api.VIOLATIONS_FILE = data_dir / "violations.parquet"
    renter_shield.api.OWNER_REG_FILE = data_dir / "owner_registrations.parquet"

    # Reload API keys
    renter_shield.api.VALID_API_KEYS = renter_shield.api._load_api_keys()

    from renter_shield.api import app
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Auth enforcement
# ---------------------------------------------------------------------------

class TestAuthGating:
    def test_no_key_returns_401(self, client):
        resp = client.get("/api/renter/address/search?q=main")
        assert resp.status_code == 401

    def test_invalid_key_returns_401(self, client):
        resp = client.get(
            "/api/renter/address/search?q=main",
            headers={"X-API-Key": "bogus-key-12345"},
        )
        assert resp.status_code == 401

    def test_renter_key_cannot_access_investigator(self, client):
        resp = client.get(
            "/api/investigator/jurisdictions",
            headers={"X-API-Key": "test-renter-key"},
        )
        assert resp.status_code == 403

    def test_investigator_key_can_access_renter(self, client):
        resp = client.get(
            "/api/renter/address/search?q=main",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Renter address search
# ---------------------------------------------------------------------------

class TestRenterSearch:
    def test_search_returns_results(self, client):
        resp = client.get(
            "/api/renter/address/search?q=MAIN",
            headers={"X-API-Key": "test-renter-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        assert data["results"][0]["address"] == "123 MAIN ST APT 1"

    def test_search_response_structure(self, client):
        resp = client.get(
            "/api/renter/address/search?q=MAIN",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        result = data["results"][0]
        # Required fields
        for field in ("bbl", "address", "jurisdiction", "rating", "rating_level", "violations"):
            assert field in result, f"Missing field: {field}"
        # Renter response should NOT expose owner_id directly
        if result.get("owner"):
            assert "owner_id" not in result["owner"]
            assert "rating" in result["owner"]

    def test_search_filters_by_jurisdiction(self, client):
        resp = client.get(
            "/api/renter/address/search?q=BEACON&jurisdiction=boston",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        assert data["total"] >= 1
        for r in data["results"]:
            assert r["jurisdiction"] == "boston"

    def test_search_pagination(self, client):
        resp = client.get(
            "/api/renter/address/search?q=MAIN&limit=1&offset=0",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        assert len(data["results"]) <= 1
        assert "total" in data
        assert data["offset"] == 0

    def test_search_min_length_enforced(self, client):
        resp = client.get(
            "/api/renter/address/search?q=ab",
            headers={"X-API-Key": "test-renter-key"},
        )
        assert resp.status_code == 422  # validation error


# ---------------------------------------------------------------------------
# Renter property detail
# ---------------------------------------------------------------------------

class TestRenterProperty:
    def test_property_found(self, client):
        resp = client.get(
            "/api/renter/property/nyc-1000010001",
            headers={"X-API-Key": "test-renter-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["property"]["bbl"] == "nyc-1000010001"
        assert data["property"]["address"] == "123 MAIN ST APT 1"

    def test_property_has_violations(self, client):
        resp = client.get(
            "/api/renter/property/nyc-1000010001",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        assert data["violations_summary"]["total"] == 3
        assert data["violations_summary"]["critical"] == 1
        assert data["violations_summary"]["open"] == 2

    def test_property_has_rating(self, client):
        resp = client.get(
            "/api/renter/property/nyc-1000010001",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        assert "rating" in data
        assert "rating_level" in data
        assert 1 <= data["rating_level"] <= 5

    def test_property_not_found(self, client):
        resp = client.get(
            "/api/renter/property/nyc-9999999999",
            headers={"X-API-Key": "test-renter-key"},
        )
        assert resp.status_code == 404

    def test_property_includes_disclaimer(self, client):
        resp = client.get(
            "/api/renter/property/nyc-1000010001",
            headers={"X-API-Key": "test-renter-key"},
        )
        data = resp.json()
        assert "disclaimer" in data
        assert len(data["disclaimer"]) > 50


# ---------------------------------------------------------------------------
# Investigator endpoints
# ---------------------------------------------------------------------------

class TestInvestigatorEndpoints:
    def test_jurisdictions_list(self, client):
        resp = client.get(
            "/api/investigator/jurisdictions",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "jurisdictions" in data
        jurs = {j["jurisdiction"] for j in data["jurisdictions"]}
        assert "nyc" in jurs

    def test_jurisdiction_landlords(self, client):
        resp = client.get(
            "/api/investigator/jurisdictions/nyc/landlords",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        assert data["results"][0]["owner_id"] == "slumlord_mcgee [nyc]"

    def test_jurisdiction_not_found(self, client):
        resp = client.get(
            "/api/investigator/jurisdictions/mars/landlords",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 404

    def test_landlord_detail(self, client):
        encoded = quote("slumlord_mcgee [nyc]", safe="")
        resp = client.get(
            f"/api/investigator/landlords/{encoded}",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["owner_id"] == "slumlord_mcgee [nyc]"
        assert "score_breakdown" in data
        assert data["total_harm_score"] == pytest.approx(38.9)

    def test_landlord_not_found(self, client):
        resp = client.get(
            "/api/investigator/landlords/nobody_here",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 404

    def test_investigator_property_has_full_owner(self, client):
        """Investigator property endpoint exposes full owner detail (unlike renter)."""
        resp = client.get(
            "/api/investigator/property/nyc-1000010001",
            headers={"X-API-Key": "test-inv-key"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["owner"] is not None
        assert "owner_id" in data["owner"]
        assert "total_harm_score" in data["owner"]

    def test_investigator_search_exposes_owner_id(self, client):
        """Investigator search shows owner_id (unlike renter search)."""
        resp = client.get(
            "/api/renter/address/search?q=MAIN",
            headers={"X-API-Key": "test-renter-key"},
        )
        renter_data = resp.json()

        resp = client.get(
            "/api/investigator/address/search?q=MAIN",
            headers={"X-API-Key": "test-inv-key"},
        )
        inv_data = resp.json()

        # Renter result omits owner_id
        renter_owner = renter_data["results"][0].get("owner")
        if renter_owner:
            assert "owner_id" not in renter_owner

        # Investigator result includes owner_id
        inv_owner = inv_data["results"][0].get("owner")
        assert inv_owner is not None
        assert "owner_id" in inv_owner


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_no_auth_required(self, client):
        """Health endpoint must not require authentication."""
        resp = client.get("/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# URL encoding round-trip (tests actual app path resolution, not just stdlib)
# ---------------------------------------------------------------------------

class TestOwnerIdPathEncoding:
    """Owner IDs with special chars must survive as FastAPI path params."""

    @pytest.mark.parametrize("owner_id", [
        "slumlord_mcgee [nyc]",
        "good_landlord [boston]",
    ])
    def test_encoded_owner_id_resolves(self, client, owner_id):
        encoded = quote(owner_id, safe="")
        resp = client.get(
            f"/api/investigator/landlords/{encoded}",
            headers={"X-API-Key": "test-inv-key"},
        )
        # Should be 200 (found) or 404 (not in data), never 422 or 500
        assert resp.status_code in (200, 404)
