"""Smoke tests — verify scoring logic, schema contracts, and config invariants."""

import polars as pl
import pytest

from renter_shield.config import (
    LIKERT_SCALE,
    SCORE_WEIGHTS,
    SEVERITY_POINTS,
    SVI_THEMES,
    MIN_DATE,
    MIN_JURISDICTION_SIZE,
)
from renter_shield.models import VIOLATIONS_SCHEMA, PROPERTIES_SCHEMA, CONTACTS_SCHEMA


# ---------------------------------------------------------------------------
# Config invariants — these protect real scoring constraints
# ---------------------------------------------------------------------------

class TestScoreWeights:
    def test_sum_to_one(self):
        assert abs(sum(SCORE_WEIGHTS.values()) - 1.0) < 1e-9

    def test_all_positive(self):
        for k, v in SCORE_WEIGHTS.items():
            assert v > 0, f"Weight '{k}' must be positive, got {v}"

    def test_expected_components(self):
        assert set(SCORE_WEIGHTS.keys()) == {"severity", "density", "widespread", "persistence"}


class TestSeverityPoints:
    def test_all_four_tiers(self):
        assert set(SEVERITY_POINTS.keys()) == {1, 2, 3, 4}

    def test_tier_ordering(self):
        assert SEVERITY_POINTS[1] > SEVERITY_POINTS[2] > SEVERITY_POINTS[3] >= SEVERITY_POINTS[4]

    def test_tier4_is_zero(self):
        assert SEVERITY_POINTS[4] == 0.0, "Informational tier should score zero"


class TestLikertScale:
    def test_covers_full_range(self):
        """Last threshold must be > 1.0 to catch score == 1.0."""
        assert LIKERT_SCALE[-1][0] > 1.0

    def test_monotonically_increasing_thresholds(self):
        thresholds = [entry[0] for entry in LIKERT_SCALE]
        assert thresholds == sorted(thresholds)

    def test_levels_are_sequential(self):
        levels = [entry[1] for entry in LIKERT_SCALE]
        assert levels == list(range(1, len(LIKERT_SCALE) + 1))

    def test_all_have_labels(self):
        for entry in LIKERT_SCALE:
            assert len(entry[2]) > 0, f"Level {entry[1]} missing label"


class TestSVIThemes:
    def test_all_components_referenced(self):
        """Every SCORE_WEIGHTS component must appear in some SVI theme."""
        all_cols = []
        for cols in SVI_THEMES.values():
            all_cols.extend(cols)
        for component in SCORE_WEIGHTS:
            assert f"{component}_score" in all_cols, (
                f"Component '{component}' not referenced in SVI_THEMES"
            )


# ---------------------------------------------------------------------------
# Schema contracts — adapters must produce these columns
# ---------------------------------------------------------------------------

class TestSchemaJoinKeys:
    def test_violations_bbl(self):
        assert "bbl" in VIOLATIONS_SCHEMA

    def test_properties_bbl(self):
        assert "bbl" in PROPERTIES_SCHEMA

    def test_join_key_chain(self):
        """Properties → Contacts join via registration_id."""
        assert "registration_id" in PROPERTIES_SCHEMA
        assert "registration_id" in CONTACTS_SCHEMA

    def test_violations_has_scoring_columns(self):
        """Scoring requires severity_tier, status, inspection_date."""
        for col in ("severity_tier", "status", "inspection_date"):
            assert col in VIOLATIONS_SCHEMA, f"Missing scoring column: {col}"

    def test_properties_has_units(self):
        """Density score requires units_residential."""
        assert "units_residential" in PROPERTIES_SCHEMA

    def test_all_schemas_have_jurisdiction(self):
        """Jurisdiction column needed for per-jurisdiction percentile pooling."""
        for name, schema in [
            ("VIOLATIONS", VIOLATIONS_SCHEMA),
            ("PROPERTIES", PROPERTIES_SCHEMA),
            ("CONTACTS", CONTACTS_SCHEMA),
        ]:
            assert "jurisdiction" in schema, f"{name}_SCHEMA missing 'jurisdiction'"


# ---------------------------------------------------------------------------
# Property scoring logic (unit tests for _compute_property_score)
# ---------------------------------------------------------------------------

class TestComputePropertyScore:
    """Test the API's property-level scoring function with controlled data."""

    @pytest.fixture
    def _import_scorer(self):
        """Import the private scoring functions from api.py."""
        from renter_shield.api import _compute_property_score, _property_likert
        return _compute_property_score, _property_likert

    def _make_viols(self, tiers_and_statuses: list[tuple[int, str]]) -> pl.DataFrame:
        """Build a minimal violations DataFrame."""
        return pl.DataFrame(
            {
                "severity_tier": [t for t, _ in tiers_and_statuses],
                "status": [s for _, s in tiers_and_statuses],
            },
            schema={"severity_tier": pl.Int8, "status": pl.Utf8},
        )

    def test_empty_violations_scores_zero(self, _import_scorer):
        score_fn, _ = _import_scorer
        result = score_fn(self._make_viols([]))
        assert result["property_score"] == 0.0
        assert result["severity_score"] == 0.0
        assert result["open_pct"] == 0.0

    def test_single_critical_open(self, _import_scorer):
        score_fn, _ = _import_scorer
        viols = self._make_viols([(1, "open")])
        result = score_fn(viols)
        # severity = 5.0, open_pct = 1.0
        # score = 5.0 * 0.8 + (1.0 * 100) * 0.2 = 4.0 + 20.0 = 24.0
        assert result["severity_score"] == 5.0
        assert result["open_pct"] == 1.0
        assert result["property_score"] == 24.0

    def test_mixed_tiers_all_closed(self, _import_scorer):
        score_fn, _ = _import_scorer
        viols = self._make_viols([(1, "closed"), (2, "closed"), (3, "closed"), (4, "closed")])
        result = score_fn(viols)
        # severity = 5.0 + 2.5 + 1.0 + 0.0 = 8.5
        # open_pct = 0
        # score = 8.5 * 0.8 + 0 = 6.8
        assert result["severity_score"] == 8.5
        assert result["open_pct"] == 0.0
        assert result["property_score"] == 6.8

    def test_all_open_increases_score(self, _import_scorer):
        score_fn, _ = _import_scorer
        closed = self._make_viols([(3, "closed"), (3, "closed")])
        open_v = self._make_viols([(3, "open"), (3, "open")])
        closed_score = score_fn(closed)["property_score"]
        open_score = score_fn(open_v)["property_score"]
        assert open_score > closed_score


class TestPropertyLikert:
    """Test Likert rating assignment from property scores."""

    @pytest.fixture
    def _import_scorer(self):
        from renter_shield.api import _compute_property_score, _property_likert
        return _compute_property_score, _property_likert

    def _make_viols(self, tiers_and_statuses: list[tuple[int, str]]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "severity_tier": [t for t, _ in tiers_and_statuses],
                "status": [s for _, s in tiers_and_statuses],
            },
            schema={"severity_tier": pl.Int8, "status": pl.Utf8},
        )

    def test_no_violations_is_level_1(self, _import_scorer):
        score_fn, likert_fn = _import_scorer
        viols = self._make_viols([])
        pv = score_fn(viols)
        result = likert_fn(pv, viols)
        assert result["level"] == 1
        assert "no issues" in result["label"].lower()

    def test_minor_closed_is_level_2(self, _import_scorer):
        score_fn, likert_fn = _import_scorer
        viols = self._make_viols([(3, "closed")])
        pv = score_fn(viols)
        result = likert_fn(pv, viols)
        assert result["level"] == 2

    def test_critical_violations_push_to_level_4_or_5(self, _import_scorer):
        score_fn, likert_fn = _import_scorer
        # Several critical violations, some open
        viols = self._make_viols([(1, "open"), (1, "open"), (1, "closed"), (2, "open")])
        pv = score_fn(viols)
        result = likert_fn(pv, viols)
        assert result["level"] >= 4

    def test_level_monotonically_increases_with_severity(self, _import_scorer):
        """More severe portfolio → equal or higher Likert level."""
        score_fn, likert_fn = _import_scorer
        scenarios = [
            [(3, "closed")],
            [(3, "open"), (3, "open")],
            [(2, "open"), (2, "open"), (3, "open")],
            [(1, "open"), (1, "open"), (2, "open"), (2, "open")],
        ]
        levels = []
        for s in scenarios:
            viols = self._make_viols(s)
            pv = score_fn(viols)
            levels.append(likert_fn(pv, viols)["level"])
        # Should be non-decreasing
        for i in range(len(levels) - 1):
            assert levels[i] <= levels[i + 1], (
                f"Likert level decreased from scenario {i} ({levels[i]}) "
                f"to scenario {i+1} ({levels[i+1]})"
            )
