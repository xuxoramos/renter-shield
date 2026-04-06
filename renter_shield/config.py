"""Global configuration: severity tiers, score weights, jurisdiction registry."""

# ---------------------------------------------------------------------------
# Analysis Time Horizon
#
# All jurisdiction adapters use this date as the lower bound for violations.
#
# Rationale for 2022-01-01:
# - COVID-19 caused most cities to pause non-emergency housing inspections
#   from ~March 2020 through mid-2021 (NYC HPD May 2021, Boston ISD June
#   2021, SF DBI March 2021, Chicago April 2021, Philadelphia May 2021).
# - Including 2020-2021 data would introduce false negatives in the
#   persistence score (inspection gaps break recurrence chains) and
#   artificial surges from post-moratorium catch-up inspections.
# - By January 2022 all cities had been fully operational for 6+ months,
#   clearing both the inspection gap and the backlog surge.
# - The 4.25-year window (Jan 2022 – Apr 2026) provides ≥3 full violation
#   recurrence cycles for fast-cycling habitability issues (heat, vermin,
#   leaks: ~6-8 months/cycle) and ≥2 cycles for slow-cycling structural
#   issues (~12-18 months/cycle), sufficient for the persistence component
#   (10% of the harm score).
# ---------------------------------------------------------------------------
MIN_DATE = "2022-01-01"

# ---------------------------------------------------------------------------
# Universal Violation Severity Tiers
# Each jurisdiction adapter maps its local codes to one of these tiers.
# ---------------------------------------------------------------------------
SEVERITY_POINTS = {
    1: 5.0,   # Critical / Immediately hazardous  (NYC Class C)
    2: 2.5,   # Serious  / Hazardous              (NYC Class B)
    3: 1.0,   # Minor    / Non-hazardous           (NYC Class A)
    4: 0.0,   # Informational                      (NYC Class I)
}

# ---------------------------------------------------------------------------
# Harm Score Weights  (must sum to 1.0)
# ---------------------------------------------------------------------------
SCORE_WEIGHTS = {
    "severity":    0.40,
    "density":     0.30,
    "widespread":  0.20,
    "persistence": 0.10,
}

# Ratio-based components are multiplied by this before weighting so their
# magnitude is comparable to the severity sum.
RATIO_SCALE = 100

# ---------------------------------------------------------------------------
# SVI-style Composite Score (Option C — hybrid percentile)
#
# After computing raw component values (severity, density, widespread,
# persistence), each component is percentile-ranked within its jurisdiction
# (or within a pooled "small jurisdictions" group if the jurisdiction has
# fewer than MIN_JURISDICTION_SIZE scored owners).
#
# Components are grouped into **themes**, averaged within-theme to produce
# a theme percentile (0-1), then averaged across themes into a composite
# score (0-1).
#
# Reference: CDC Social Vulnerability Index methodology
#            (Flanagan et al., 2011; ATSDR/CDC SVI Documentation 2018)
# ---------------------------------------------------------------------------

# Minimum number of scored owners in a jurisdiction for it to use its
# own percentile distribution.  Smaller jurisdictions are pooled together.
MIN_JURISDICTION_SIZE = 50

# Theme groupings.  Each key is a theme name; values are the raw component
# columns computed by the DuckDB scoring step.
SVI_THEMES = {
    "severity":   ["severity_score"],
    "portfolio":  ["density_score", "widespread_score"],
    "compliance": ["persistence_score"],
}

# Likert thresholds on the 0-1 composite.  Each tuple is (upper_bound, level,
# label, color).  Evaluated in order; first match wins.
LIKERT_SCALE = [
    (0.20, 1, "Low concern",          "🟢"),
    (0.40, 2, "Some concerns",        "🟡"),
    (0.60, 3, "Moderate concerns",    "🟠"),
    (0.80, 4, "Significant concerns", "🔴"),
    (1.01, 5, "Severe concerns",      "🔴"),  # 1.01 to catch score == 1.0
]

# ---------------------------------------------------------------------------
# Jurisdiction registry — maps a short code to its adapter class path.
# New cities are added here; the pipeline discovers them at runtime.
# ---------------------------------------------------------------------------
JURISDICTION_REGISTRY: dict[str, str] = {
    "nyc": "renter_shield.jurisdictions.nyc.NYCAdapter",
    "chicago": "renter_shield.jurisdictions.chicago.ChicagoAdapter",
    "philadelphia": "renter_shield.jurisdictions.philadelphia.PhiladelphiaAdapter",
    "sf": "renter_shield.jurisdictions.sf.SFAdapter",
    "boston": "renter_shield.jurisdictions.boston.BostonAdapter",
    "seattle": "renter_shield.jurisdictions.seattle.SeattleAdapter",
    "pittsburgh": "renter_shield.jurisdictions.pittsburgh.PittsburghAdapter",
    "hud_reac": "renter_shield.jurisdictions.hud_reac.HUDREACAdapter",
    "la": "renter_shield.jurisdictions.la.LAAdapter",
    "austin": "renter_shield.jurisdictions.austin.AustinAdapter",
    "miami": "renter_shield.jurisdictions.miami.MiamiAdapter",
    "detroit": "renter_shield.jurisdictions.detroit.DetroitAdapter",
}
