"""Smoke tests — verify core modules import and config is consistent."""

from renter_shield.config import SCORE_WEIGHTS, SEVERITY_POINTS, MIN_DATE
from renter_shield.models import VIOLATIONS_SCHEMA, PROPERTIES_SCHEMA, CONTACTS_SCHEMA


def test_score_weights_sum_to_one():
    assert abs(sum(SCORE_WEIGHTS.values()) - 1.0) < 1e-9


def test_severity_tiers_complete():
    assert set(SEVERITY_POINTS.keys()) == {1, 2, 3, 4}
    assert SEVERITY_POINTS[1] > SEVERITY_POINTS[2] > SEVERITY_POINTS[3] >= SEVERITY_POINTS[4]


def test_schemas_have_required_join_keys():
    assert "bbl" in VIOLATIONS_SCHEMA
    assert "bbl" in PROPERTIES_SCHEMA
    assert "registration_id" in PROPERTIES_SCHEMA
    assert "registration_id" in CONTACTS_SCHEMA


def test_min_date_format():
    from datetime import date
    d = date.fromisoformat(MIN_DATE)
    assert d.year >= 2022
