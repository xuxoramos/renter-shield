"""Web UI tests — cookie auth flow, page rendering, htmx fragments.

Tests the full renter web experience:
  - Registration → cookie → access protected pages
  - Search via htmx fragment endpoint
  - Property detail rendering
  - Redirect behavior when unauthenticated
  - Edge cases (invalid tokens, missing data)
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def data_dir():
    """Create temp Parquet files for the web UI."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)

        properties = pl.DataFrame({
            "bbl": ["nyc-1000010001", "nyc-1000020002"],
            "registration_id": ["reg-001", "reg-002"],
            "units_residential": [10.0, 5.0],
            "year_built": ["1920", "2005"],
            "address": ["123 MAIN ST APT 1", "456 BROADWAY"],
            "jurisdiction": ["nyc", "nyc"],
        })
        properties.write_parquet(outdir / "properties.parquet")

        violations = pl.DataFrame({
            "violation_id": ["v1", "v2", "v3"],
            "bbl": ["nyc-1000010001", "nyc-1000010001", "nyc-1000020002"],
            "severity_tier": pl.Series([1, 3, 2], dtype=pl.Int8),
            "status": ["open", "closed", "open"],
            "inspection_date": [date(2023, 1, 15), date(2023, 3, 10), date(2024, 2, 20)],
            "jurisdiction": ["nyc", "nyc", "nyc"],
        })
        violations.write_parquet(outdir / "violations.parquet")

        owner_regs = pl.DataFrame({
            "owner_id": ["test_owner [nyc]", "test_owner [nyc]"],
            "jurisdiction": ["nyc", "nyc"],
            "registration_id": ["reg-001", "reg-002"],
            "confidence": ["high", "high"],
        })
        owner_regs.write_parquet(outdir / "owner_registrations.parquet")

        scores = pl.DataFrame({
            "owner_id": ["test_owner [nyc]"],
            "jurisdiction": ["nyc"],
            "confidence": ["high"],
            "num_properties": [2],
            "total_violations": [3],
            "class_c_violations": [1],
            "severity_score": [8.5],
            "density_score": [0.2],
            "widespread_score": [1.0],
            "persistence_score": [0.33],
            "total_units": [15],
            "unresolved_violations": [2],
            "total_harm_score": [25.0],
            "svi_composite": [0.65],
            "likert_level": [4],
            "likert_label": ["Significant concerns"],
        })
        scores.write_parquet(outdir / "all_landlords_harm_scores.parquet")

        yield outdir


@pytest.fixture(scope="module")
def client(data_dir):
    os.environ["LI_OUTPUT_DIR"] = str(data_dir)
    os.environ["LI_API_KEYS"] = "unused:renter"

    import renter_shield.web
    import renter_shield.api

    # Reset web module's data cache so it reloads from new OUTPUT_DIR
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

    from renter_shield.api import app
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Registration flow
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_page_renders(self, client):
        resp = client.get("/renter/register")
        assert resp.status_code == 200
        assert "Register" in resp.text or "register" in resp.text.lower()

    def test_register_creates_cookie(self, client):
        resp = client.post(
            "/renter/register",
            data={"name": "Test User", "email": "test@example.com", "role": "", "agree": "on"},
        )
        assert resp.status_code == 200
        assert "rs_token" in resp.headers.get("set-cookie", "")

    def test_register_missing_agree_fails(self, client):
        """Form without the 'agree' checkbox should not create a session."""
        resp = client.post(
            "/renter/register",
            data={"name": "Test", "email": "bad@test.com", "role": ""},
        )
        # Should either return a 422 or re-render the form (200 with error)
        # Not a redirect to authenticated page
        assert "rs_token" not in resp.headers.get("set-cookie", "")


# ---------------------------------------------------------------------------
# Auth redirect behavior
# ---------------------------------------------------------------------------

class TestAuthRedirects:
    def test_search_page_redirects_without_cookie(self, client):
        # Use a fresh client without any cookies to test unauthenticated access
        from renter_shield.api import app
        fresh = TestClient(app, raise_server_exceptions=False, cookies={})
        resp = fresh.get("/renter/", follow_redirects=False)
        assert resp.status_code in (302, 303, 307)
        assert "/renter/register" in resp.headers.get("location", "")

    def test_property_page_redirects_without_cookie(self, client):
        from renter_shield.api import app
        fresh = TestClient(app, raise_server_exceptions=False, cookies={})
        resp = fresh.get("/renter/property/nyc-1000010001", follow_redirects=False)
        assert resp.status_code in (302, 303, 307)

    def test_invalid_cookie_redirects(self, client):
        from renter_shield.api import app
        fresh = TestClient(app, raise_server_exceptions=False, cookies={"rs_token": "totally-bogus-token"})
        resp = fresh.get("/renter/", follow_redirects=False)
        assert resp.status_code in (302, 303, 307)


# ---------------------------------------------------------------------------
# Authenticated page rendering
# ---------------------------------------------------------------------------

class TestAuthenticatedPages:
    @pytest.fixture
    def auth_cookie(self, client):
        """Register and extract the auth cookie."""
        resp = client.post(
            "/renter/register",
            data={"name": "Page Tester", "email": "pages@test.com", "role": "", "agree": "on"},
        )
        cookie_header = resp.headers.get("set-cookie", "")
        token = cookie_header.split("rs_token=")[1].split(";")[0]
        return {"rs_token": token}

    def test_search_page_loads(self, client, auth_cookie):
        resp = client.get("/renter/", cookies=auth_cookie)
        assert resp.status_code == 200
        assert "Look Up an Address" in resp.text or "search" in resp.text.lower()

    def test_search_fragment_returns_results(self, client, auth_cookie):
        resp = client.get("/renter/search?q=MAIN", cookies=auth_cookie)
        assert resp.status_code == 200
        assert "123 MAIN ST" in resp.text

    def test_search_fragment_no_results(self, client, auth_cookie):
        resp = client.get("/renter/search?q=ZZZZNONEXISTENT", cookies=auth_cookie)
        assert resp.status_code == 200
        # Should render the "no results" state, not crash
        assert "No" in resp.text or "no" in resp.text

    def test_property_page_renders(self, client, auth_cookie):
        resp = client.get("/renter/property/nyc-1000010001", cookies=auth_cookie)
        assert resp.status_code == 200
        assert "123 MAIN ST" in resp.text
        # Should show violation info
        assert "Critical" in resp.text or "critical" in resp.text.lower() or "Tier 1" in resp.text

    def test_property_page_404_for_missing(self, client, auth_cookie):
        resp = client.get("/renter/property/nyc-9999999999", cookies=auth_cookie)
        assert resp.status_code == 404

    def test_owner_page_renders(self, client, auth_cookie):
        resp = client.get("/renter/owner/test_owner%20%5Bnyc%5D", cookies=auth_cookie)
        assert resp.status_code == 200
        # Should show the owner's properties
        assert "123 MAIN ST" in resp.text or "456 BROADWAY" in resp.text

    def test_owner_page_404_for_missing(self, client, auth_cookie):
        resp = client.get("/renter/owner/nobody_here", cookies=auth_cookie)
        assert resp.status_code == 404

    def test_violations_pagination_fragment(self, client, auth_cookie):
        resp = client.get(
            "/renter/property/nyc-1000010001/violations?page=1",
            cookies=auth_cookie,
        )
        assert resp.status_code == 200
        # Should be an HTML fragment (not full page)
        assert "<html" not in resp.text.lower() or "<!doctype" not in resp.text.lower()


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------

class TestStaticFiles:
    def test_css_served(self, client):
        resp = client.get("/static/style.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers.get("content-type", "")

    def test_htmx_served(self, client):
        resp = client.get("/static/htmx.min.js")
        assert resp.status_code == 200
        assert "javascript" in resp.headers.get("content-type", "")
