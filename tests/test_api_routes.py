"""API route tests — verify all registered routes exist and are not 404.

These tests use FastAPI's TestClient which bypasses nginx; they confirm that
the routes are registered and gated by authentication (401), not missing (404).

The renter app's address-detail view now links owners to its own
``?page=owner&owner=<id>`` page (handled within the renter Streamlit) instead
of cross-linking to the investigator Streamlit at ``/investigator/``, which
previously returned 404 when the investigator process was unavailable or
when the user lacked investigator-level access.

The FastAPI ``/investigator/`` prefix still has no root handler (only
sub-routes like ``/investigator/jurisdictions``).  Tests below verify that
sub-routes are auth-gated (401) rather than missing (404).
"""

from urllib.parse import quote, unquote

from fastapi.testclient import TestClient

from renter_shield.api import app

client = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Health check (unauthenticated, always 200)
# ---------------------------------------------------------------------------

def test_health_endpoint():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Renter routes — require any valid API key (401 without one, not 404)
# ---------------------------------------------------------------------------

def test_renter_address_search_exists():
    resp = client.get("/renter/address/search?q=test")
    assert resp.status_code == 401, (
        "Route /renter/address/search must exist (401 auth required, not 404 missing)"
    )


def test_renter_property_exists():
    resp = client.get("/renter/property/nyc-1234567890")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Investigator routes — require investigator-scoped API key (401, not 404)
# ---------------------------------------------------------------------------

def test_investigator_jurisdictions_exists():
    resp = client.get("/investigator/jurisdictions")
    assert resp.status_code == 401, (
        "Route /investigator/jurisdictions must exist (401, not 404)"
    )


def test_investigator_jurisdiction_landlords_exists():
    resp = client.get("/investigator/jurisdictions/nyc/landlords")
    assert resp.status_code == 401


def test_investigator_address_search_exists():
    resp = client.get("/investigator/address/search?q=test")
    assert resp.status_code == 401


def test_investigator_property_exists():
    resp = client.get("/investigator/property/nyc-1234567890")
    assert resp.status_code == 401


def test_investigator_landlords_search_exists():
    resp = client.get("/investigator/landlords/search")
    assert resp.status_code == 401


def test_investigator_landlords_by_id_exists():
    """FastAPI route GET /investigator/landlords/{owner_id} must be registered.

    This verifies the FastAPI route exists and is auth-gated (401), not missing
    (404).  In the production deployment callers reach this endpoint at
    GET /api/investigator/landlords/{owner_id} (nginx strips the /api/ prefix).
    """
    resp = client.get("/investigator/landlords/john_smith%20%5Bnyc%5D")
    assert resp.status_code == 401, (
        "Route /investigator/landlords/{owner_id} must exist (401, not 404). "
        "A 404 here means the route was accidentally removed."
    )


# ---------------------------------------------------------------------------
# Owner link URL-encoding round-trip
# ---------------------------------------------------------------------------

def test_owner_link_url_encoding():
    """Verify that owner IDs with special characters survive percent-encoding.

    The investigator Streamlit (and the FastAPI landlords endpoint) receive
    owner IDs via URL query parameters or path segments.  This test confirms
    that ``quote(owner_id, safe='')`` round-trips losslessly for representative
    owner ID formats.
    """
    owner_ids = [
        "john_smith [nyc]",
        "acme/realty llc [boston]",
        "o'brien & sons [chicago]",
        "east 92nd st mgmt [nyc]",
    ]

    for owner_id in owner_ids:
        encoded = quote(owner_id, safe="")
        # No raw special characters should appear in the encoded query param
        assert " " not in encoded, f"Space not encoded for {owner_id!r}"
        assert "[" not in encoded, f"'[' not encoded for {owner_id!r}"
        assert "]" not in encoded, f"']' not encoded for {owner_id!r}"
        assert "/" not in encoded, f"'/' not encoded for {owner_id!r}"
        assert "&" not in encoded, f"'&' not encoded for {owner_id!r}"

        # Must round-trip losslessly
        assert unquote(encoded) == owner_id, (
            f"Round-trip failed for {owner_id!r}: "
            f"encoded={encoded!r}, decoded={unquote(encoded)!r}"
        )
