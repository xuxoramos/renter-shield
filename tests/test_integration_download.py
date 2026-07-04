"""Integration tests — exercise the real ``download()`` path against live
jurisdiction APIs and verify the normalized ``load_*`` outputs.

These catch the class of failures that unit tests cannot: cross-page schema
drift, type-inference mismatches between batches, and open/closed dataset
column differences.  They are slow and network-dependent, so they are marked
``integration`` and skipped by default.  Run explicitly with::

    pytest -m integration

or on a schedule via the ``integration`` CI workflow.

Skips gracefully if ``sodapy`` (the download extra) is not installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("sodapy", reason="requires the [download] extra")

from renter_shield.config import JURISDICTION_REGISTRY
from renter_shield.models import (
    CONTACTS_SCHEMA,
    PROPERTIES_SCHEMA,
    VIOLATIONS_SCHEMA,
)
from renter_shield.pipeline import _load_adapter

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("jurisdiction", list(JURISDICTION_REGISTRY))
def test_download_and_normalize(jurisdiction, tmp_path):
    """Download live data, then verify each table normalizes cleanly.

    Failure modes this guards against:
      - schema drift across paginated batches (diagonal_relaxed concat)
      - type inference differences between batches (infer_schema_length)
      - open/closed dataset column-count mismatches (LA)
      - load-time concat of per-source frames (Miami)
    """
    adapter = _load_adapter(jurisdiction, tmp_path)

    # 1. Download must complete without raising (this is where the concat
    #    schema-drift errors surfaced in production).
    adapter.download()

    # 2. Each normalized table must collect and expose the required columns.
    violations = adapter.load_violations().collect()
    assert set(VIOLATIONS_SCHEMA).issubset(violations.columns), (
        f"{jurisdiction}: violations missing columns "
        f"{set(VIOLATIONS_SCHEMA) - set(violations.columns)}"
    )

    properties = adapter.load_properties().collect()
    assert set(PROPERTIES_SCHEMA).issubset(properties.columns), (
        f"{jurisdiction}: properties missing columns "
        f"{set(PROPERTIES_SCHEMA) - set(properties.columns)}"
    )

    # Contacts may legitimately be empty (jurisdictions without owner data),
    # but the schema contract must still hold.
    contacts = adapter.load_contacts().collect()
    assert set(CONTACTS_SCHEMA).issubset(contacts.columns), (
        f"{jurisdiction}: contacts missing columns "
        f"{set(CONTACTS_SCHEMA) - set(contacts.columns)}"
    )
