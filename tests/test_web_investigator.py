"""Investigator web UI tests — registration, overview, jurisdiction, owner detail.

Tests the full investigator web experience:
  - Registration flow (investigator scope)
  - Auth redirects for unauthenticated access
  - Overview page with jurisdiction cards
  - Jurisdiction detail with owners table
  - Owner detail with score breakdown
  - Cross-jurisdiction search fragment
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def data_dir():
    """Create temp Parquet files for the investigator web UI."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)

        properties = pl.DataFrame({
            "bbl": ["nyc-1000010001", "nyc-1000020002", "bos-100001"],
            "registration_id": ["reg-001", "reg-002", "reg-003"],
            "units_residential": [10.0, 5.0, 8.0],
            "year_built": ["1920", "2005", "1985"],
            "address": ["123 MAIN ST APT 1", "456 BROADWAY", "789 BEACON ST"],
            "jurisdiction": ["nyc", "nyc", "boston"],
        })
        properties.write_parquet(outdir / "properties.parquet")

        violations = pl.DataFrame({
            "violation_id": ["v1", "v2", "v3", "v4"],
            "bbl": ["nyc-1000010001", "nyc-1000010001", "nyc-1000020002", "bos-100001"],
            "severity_tier": pl.Series([1, 3, 2, 1], dtype=pl.Int8),
            "status": ["open", "closed", "open", "open"],
            "jurisdiction": ["nyc", "nyc", "nyc", "boston"],
        })
        violations.write_parquet(outdir / "violations.parquet")

        scores = pl.DataFrame({
            "owner_id": ["WALNUT CAPITAL [nyc]", "SMITH REALTY [nyc]", "BEACON MGMT [boston]"],
            "jurisdiction": ["nyc", "nyc", "boston"],
            "confidence": ["high", "medium", "low"],
            "num_properties": [5, 3, 4],
            "total_violations": [20, 8, 12],
            "class_c_violations": [5, 1, 3],
            "severity_score": [42.5, 12.0, 25.0],
            "density_score": [0.4, 0.16, 0.3],
            "widespread_score": [0.8, 0.67, 0.75],
            "persistence_score": [0.5, 0.25, 0.42],
            "total_units": [50, 25, 40],
            "unresolved_violations": [10, 2, 5],
            "total_harm_score": [150.0, 45.0, 95.0],
            "svi_composite": [0.85, 0.42, 0.68],
            "likert_level": [5, 3, 4],
            "likert_label": ["Severe concerns", "Some concerns", "Moderate concerns"],
            "likert_color": ["🔴", "🟡", "🟠"],
            "theme_severity": [0.9, 0.3, 0.7],
            "theme_portfolio": [0.8, 0.4, 0.6],
            "theme_compliance": [0.85, 0.5, 0.65],
        })
        scores.write_parquet(outdir / "all_landlords_harm_scores.parquet")

        yield outdir


@pytest.fixture(scope="module")
def client(data_dir):
    os.environ["LI_OUTPUT_DIR"] = str(data_dir)
    os.environ["LI_API_KEYS"] = "unused:renter"

    import renter_shield.web
    import renter_shield.web_investigator
    import renter_shield.api

    # Reset web module's data cache
    renter_shield.web._scores_df = None
    renter_shield.web._props_df = None
    renter_shield.web._viols_df = None
    renter_shield.web._owner_reg_df = None
    renter_shield.web.OUTPUT_DIR = data_dir

    # Reset investigator web module's data cache
    renter_shield.web_investigator._scores_df = None
    renter_shield.web_investigator._props_df = None
    renter_shield.web_investigator._viols_df = None
    renter_shield.web_investigator.OUTPUT_DIR = data_dir

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


def _register_investigator(client) -> str:
    """Register an investigator and return the auth cookie token."""
    resp = client.post("/investigator/register", data={
        "name": "Test Investigator",
        "email": "inv@agency.gov",
        "role": "Investigator",
        "agree": "on",
    })
    assert resp.status_code == 200
    # Extract token from cookie
    token = resp.cookies.get("rs_token")
    assert token is not None
    return token


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
class TestInvRegistration:
    def test_register_page_renders(self, client):
        resp = client.get("/investigator/register")
        assert resp.status_code == 200
        assert "Investigator" in resp.text

    def test_register_requires_agreement(self, client):
        resp = client.post("/investigator/register", data={
            "name": "X", "email": "x@x.com",
        }, follow_redirects=False)
        assert resp.status_code == 200
        assert "must agree" in resp.text.lower()

    def test_register_requires_name_email(self, client):
        resp = client.post("/investigator/register", data={
            "name": "  ", "email": "  ", "agree": "on",
        }, follow_redirects=False)
        assert resp.status_code == 200
        assert "required" in resp.text.lower()

    def test_register_success(self, client):
        resp = client.post("/investigator/register", data={
            "name": "Jane Investigator",
            "email": "jane@oag.gov",
            "role": "AG Staff",
            "agree": "on",
        })
        assert resp.status_code == 200
        assert "rs_token" in resp.cookies
        assert "registered" in resp.text.lower() or "token" in resp.text.lower()


# ---------------------------------------------------------------------------
# Auth redirects
# ---------------------------------------------------------------------------
class TestInvAuthRedirects:
    def test_overview_requires_auth(self, client):
        c = TestClient(client.app, cookies={})
        resp = c.get("/investigator/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/investigator/register" in resp.headers["location"]

    def test_jurisdiction_requires_auth(self, client):
        c = TestClient(client.app, cookies={})
        resp = c.get("/investigator/jurisdiction/nyc", follow_redirects=False)
        assert resp.status_code == 302

    def test_owner_requires_auth(self, client):
        c = TestClient(client.app, cookies={})
        resp = c.get("/investigator/owner/WALNUT%20CAPITAL%20%5Bnyc%5D", follow_redirects=False)
        assert resp.status_code == 302

    def test_renter_token_rejected(self, client):
        """A renter-scoped token should not access investigator pages."""
        # Register as renter
        resp = client.post("/renter/register", data={
            "name": "Renter User", "email": "r@r.com", "agree": "on",
        })
        renter_token = resp.cookies.get("rs_token")
        assert renter_token
        # Try investigator page with renter cookie
        c = TestClient(client.app, cookies={"rs_token": renter_token})
        resp = c.get("/investigator/", follow_redirects=False)
        assert resp.status_code == 302


# ---------------------------------------------------------------------------
# Authenticated pages
# ---------------------------------------------------------------------------
class TestInvPages:
    @pytest.fixture(autouse=True)
    def setup(self, client):
        token = _register_investigator(client)
        self.client = TestClient(client.app, cookies={"rs_token": token})

    def test_overview_page_loads(self):
        resp = self.client.get("/investigator/")
        assert resp.status_code == 200
        assert "Landlord Harm Score Explorer" in resp.text
        # Should show jurisdiction cards
        assert "New York City" in resp.text
        assert "Boston" in resp.text

    def test_overview_shows_stats(self):
        resp = self.client.get("/investigator/")
        assert "Scored Owners" in resp.text
        assert "Total Violations" in resp.text

    def test_jurisdiction_page_scored(self):
        resp = self.client.get("/investigator/jurisdiction/nyc")
        assert resp.status_code == 200
        assert "New York City" in resp.text
        assert "Ranked Owners" in resp.text
        # Should show at least one owner
        assert "WALNUT CAPITAL" in resp.text

    def test_jurisdiction_page_filters(self):
        """Owners table fragment respects min_props filter."""
        resp = self.client.get("/investigator/jurisdiction/nyc/owners?min_props=4")
        assert resp.status_code == 200
        # WALNUT CAPITAL has 5 properties, SMITH has 3
        assert "WALNUT CAPITAL" in resp.text
        assert "SMITH REALTY" not in resp.text

    def test_jurisdiction_page_name_filter(self):
        resp = self.client.get("/investigator/jurisdiction/nyc/owners?min_props=1&name=WALNUT")
        assert resp.status_code == 200
        assert "WALNUT CAPITAL" in resp.text
        assert "SMITH" not in resp.text

    def test_owner_detail_page(self):
        import urllib.parse
        owner_id = urllib.parse.quote("WALNUT CAPITAL [nyc]", safe="")
        resp = self.client.get(f"/investigator/owner/{owner_id}")
        assert resp.status_code == 200
        assert "WALNUT CAPITAL" in resp.text
        assert "Severity" in resp.text
        assert "Density" in resp.text
        # Score breakdown
        assert "150" in resp.text  # harm score

    def test_owner_detail_shows_svi(self):
        import urllib.parse
        owner_id = urllib.parse.quote("WALNUT CAPITAL [nyc]", safe="")
        resp = self.client.get(f"/investigator/owner/{owner_id}")
        assert "0.85" in resp.text  # svi_composite
        assert "Severity Theme" in resp.text

    def test_owner_not_found(self):
        import urllib.parse
        owner_id = urllib.parse.quote("NONEXISTENT OWNER", safe="")
        resp = self.client.get(f"/investigator/owner/{owner_id}")
        assert resp.status_code == 404

    def test_search_fragment(self):
        resp = self.client.get("/investigator/search?q=WALNUT")
        assert resp.status_code == 200
        assert "WALNUT CAPITAL" in resp.text
        assert "New York City" in resp.text

    def test_search_no_results(self):
        resp = self.client.get("/investigator/search?q=ZZZZZ")
        assert resp.status_code == 200
        assert "No owners found" in resp.text

    def test_sign_out(self):
        resp = self.client.get("/investigator/sign-out", follow_redirects=False)
        assert resp.status_code == 302
        assert "/investigator/register" in resp.headers["location"]
