"""FastAPI application — dual-audience API for housing data.

Two route namespaces served from the same data layer:

  /renter/       — address-first property lookups for prospective tenants.
                   Responses omit internal identifiers (owner_id, confidence)
                   and score breakdowns.  Accessible by *any* valid API key.

  /investigator/ — owner-centric harm-score queries for housing
                   investigations.  Requires an API key with 'investigator'
                   scope.

API-key format (env ``LI_API_KEYS`` or ``api_keys.txt``):
    key:scope          e.g.  abc123:investigator   or   xyz789:renter
    key                scope defaults to 'investigator' (backward-compat)

Run with:
    uvicorn renter_shield.api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from renter_shield.config import SEVERITY_POINTS
from renter_shield import audit

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(os.getenv("LI_OUTPUT_DIR", "output"))
SCORES_FILE = OUTPUT_DIR / "all_landlords_harm_scores.parquet"

DISCLAIMER = (
    "Scores are derived from publicly available government records and "
    "algorithmic analysis. They do not constitute legal findings, may not "
    "reflect current conditions, and may contain errors due to name-based "
    "ownership resolution. Independent verification is required before "
    "any legal, administrative, or public action."
)

# ---------------------------------------------------------------------------
# API key loading — key:scope pairs
# ---------------------------------------------------------------------------
_API_KEYS_FILE = Path(os.getenv("LI_API_KEYS_FILE", "api_keys.txt"))
_API_KEYS_ENV = os.getenv("LI_API_KEYS", "")

VALID_SCOPES = {"renter", "investigator"}


def _load_api_keys() -> dict[str, str]:
    """Return ``{key: scope}`` mapping.  Bare keys default to 'investigator'."""
    keys: dict[str, str] = {}

    def _parse(raw: str) -> None:
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            return
        if ":" in raw:
            k, s = raw.rsplit(":", 1)
            k, s = k.strip(), s.strip().lower()
            if s not in VALID_SCOPES:
                s = "investigator"
            keys[k] = s
        else:
            keys[raw] = "investigator"

    if _API_KEYS_ENV:
        for part in _API_KEYS_ENV.split(","):
            _parse(part)
    if _API_KEYS_FILE.exists():
        for line in _API_KEYS_FILE.read_text().splitlines():
            _parse(line)
    return keys


VALID_API_KEYS: dict[str, str] = _load_api_keys()

# ---------------------------------------------------------------------------
# Auth dependencies
# ---------------------------------------------------------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def _verify_api_key(
    api_key: str | None = Security(_api_key_header),
) -> str:
    """Validate key and return its scope ('renter' or 'investigator').

    Checks env-var/file keys first, then SQLite-registered tokens.
    """
    if api_key is None:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

    # 1. Check legacy env-var / file-based keys
    if api_key in VALID_API_KEYS:
        return VALID_API_KEYS[api_key]

    # 2. Check SQLite-registered tokens
    user = audit.validate_token(api_key)
    if user:
        return user["scope"]

    raise HTTPException(status_code=401, detail="Invalid or missing API key.")


async def _require_investigator(
    scope: Annotated[str, Depends(_verify_api_key)],
) -> str:
    """Gate that rejects non-investigator keys."""
    if scope != "investigator":
        raise HTTPException(
            status_code=403,
            detail="This endpoint requires an investigator-scoped API key.",
        )
    return scope


# ---------------------------------------------------------------------------
# Data layer (lazy-loaded singletons)
# ---------------------------------------------------------------------------
_scores_df: pl.DataFrame | None = None
_properties_df: pl.DataFrame | None = None
_violations_df: pl.DataFrame | None = None
_owner_reg_df: pl.DataFrame | None = None

PROPERTIES_FILE = OUTPUT_DIR / "properties.parquet"
VIOLATIONS_FILE = OUTPUT_DIR / "violations.parquet"
OWNER_REG_FILE = OUTPUT_DIR / "owner_registrations.parquet"


def _get_scores() -> pl.DataFrame:
    global _scores_df  # noqa: PLW0603
    if _scores_df is None:
        if not SCORES_FILE.exists():
            raise HTTPException(
                status_code=503,
                detail=f"Scores file not found: {SCORES_FILE}. Run the pipeline first.",
            )
        _scores_df = pl.read_parquet(SCORES_FILE)
    return _scores_df


def _get_properties() -> pl.DataFrame:
    global _properties_df  # noqa: PLW0603
    if _properties_df is None:
        if not PROPERTIES_FILE.exists():
            raise HTTPException(status_code=503, detail="Properties file not found.")
        _properties_df = pl.read_parquet(PROPERTIES_FILE)
    return _properties_df


def _get_violations() -> pl.DataFrame:
    global _violations_df  # noqa: PLW0603
    if _violations_df is None:
        if not VIOLATIONS_FILE.exists():
            raise HTTPException(status_code=503, detail="Violations file not found.")
        _violations_df = pl.read_parquet(VIOLATIONS_FILE)
    return _violations_df


def _get_owner_registrations() -> pl.DataFrame:
    global _owner_reg_df  # noqa: PLW0603
    if _owner_reg_df is None:
        if not OWNER_REG_FILE.exists():
            raise HTTPException(status_code=503, detail="Owner registrations file not found.")
        _owner_reg_df = pl.read_parquet(OWNER_REG_FILE)
    return _owner_reg_df


def _compute_property_score(prop_viols: pl.DataFrame) -> dict:
    """Property-level violation score using the same severity weights."""
    n = len(prop_viols)
    if n == 0:
        return {"property_score": 0.0, "severity_score": 0.0, "open_pct": 0.0}
    sev_sum = sum(
        SEVERITY_POINTS.get(row["severity_tier"], 0)
        for row in prop_viols.iter_rows(named=True)
    )
    n_open = len(prop_viols.filter(pl.col("status") == "open"))
    open_pct = n_open / n
    score = sev_sum * 0.8 + (open_pct * 100) * 0.2
    return {
        "property_score": round(score, 1),
        "severity_score": round(sev_sum, 1),
        "open_pct": round(open_pct, 3),
    }


def _property_likert(pv: dict, prop_viols: pl.DataFrame) -> dict:
    """Map property violation summary to a 1-5 Likert rating."""
    score = pv["property_score"]
    n_total = len(prop_viols)
    n_crit = len(prop_viols.filter(pl.col("severity_tier") == 1)) if n_total else 0
    n_open = len(prop_viols.filter(pl.col("status") == "open")) if n_total else 0
    open_pct = pv["open_pct"]

    if n_total == 0:
        return {"level": 1, "label": "No issues found"}
    if n_crit == 0 and n_open == 0 and score < 10:
        return {"level": 2, "label": "Minor issues"}
    if n_crit == 0 and (n_open > 0 or score >= 10) and score < 50:
        return {"level": 3, "label": "Some concerns"}
    if (n_crit > 0 or score >= 50) and not (n_crit > 0 and open_pct > 0.3):
        return {"level": 4, "label": "Significant issues"}
    return {"level": 5, "label": "Severe issues"}


# ---------------------------------------------------------------------------
# Shared helpers for address/property lookups (used by both audiences)
# ---------------------------------------------------------------------------
def _address_matches(
    q: str,
    jurisdiction: str | None,
    limit: int,
    offset: int,
) -> tuple[int, pl.DataFrame]:
    props = _get_properties()
    matches = props.filter(
        pl.col("address").is_not_null()
        & pl.col("address").str.to_uppercase().str.contains(q.upper())
    )
    if jurisdiction:
        matches = matches.filter(pl.col("jurisdiction") == jurisdiction)
    matches = matches.sort("jurisdiction", "address")
    return len(matches), matches.slice(offset, limit)


def _owner_lookup(reg_id: str | None) -> tuple[str | None, dict | None, dict | None]:
    """Return (owner_id, score_row_dict, owner_reg_row) or Nones."""
    if not reg_id:
        return None, None, None
    oreg = _get_owner_registrations()
    scores = _get_scores()
    owner_match = oreg.filter(pl.col("registration_id") == reg_id)
    if len(owner_match) == 0:
        return None, None, None
    owner_id = owner_match["owner_id"][0]
    confidence = owner_match["confidence"][0]
    score_match = scores.filter(pl.col("owner_id") == owner_id)
    if len(score_match) == 0:
        return owner_id, None, {"owner_id": owner_id, "confidence": confidence}
    return owner_id, score_match.row(0, named=True), {"owner_id": owner_id, "confidence": confidence}


# ===================================================================
# RENTER ROUTER  — /renter/...
# Any valid API key (renter or investigator) can access these routes.
# Responses omit internal identifiers and score methodology details.
# ===================================================================
renter_router = APIRouter(prefix="/renter", tags=["renter"])


@renter_router.get("/address/search")
async def renter_search_address(
    _scope: Annotated[str, Depends(_verify_api_key)],
    q: str = Query(..., min_length=3, description="Address substring (case-insensitive)"),
    jurisdiction: str | None = Query(None, description="Filter by jurisdiction code"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search properties by address — renter-facing, no internal IDs."""
    viols = _get_violations()
    total, page = _address_matches(q, jurisdiction, limit, offset)

    results = []
    for row in page.iter_rows(named=True):
        bbl = row["bbl"]
        prop_viols = viols.filter(pl.col("bbl") == bbl)
        pv_score = _compute_property_score(prop_viols)
        pv_likert = _property_likert(pv_score, prop_viols)

        # Simplified owner signal: Likert + portfolio size, no internal IDs
        _, score_row, _ = _owner_lookup(row.get("registration_id"))
        owner_signal = None
        if score_row:
            owner_signal = {
                "rating": score_row.get("likert_label", "Unknown"),
                "rating_level": score_row.get("likert_level"),
                "properties_managed": score_row["num_properties"],
                "total_violations": score_row["total_violations"],
            }

        results.append({
            "bbl": bbl,
            "address": row["address"],
            "jurisdiction": row["jurisdiction"],
            "units_residential": row.get("units_residential"),
            "year_built": row.get("year_built"),
            "rating": pv_likert["label"],
            "rating_level": pv_likert["level"],
            "violations": len(prop_viols),
            "critical_violations": len(prop_viols.filter(pl.col("severity_tier") == 1)),
            "open_violations": len(prop_viols.filter(pl.col("status") == "open")),
            "owner": owner_signal,
        })

    return {
        "disclaimer": DISCLAIMER,
        "query": q,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


@renter_router.get("/property/{bbl}")
async def renter_get_property(
    bbl: str,
    _scope: Annotated[str, Depends(_verify_api_key)],
) -> dict:
    """Property detail for renters — violations + simplified owner signal."""
    props = _get_properties()
    viols = _get_violations()

    prop_match = props.filter(pl.col("bbl") == bbl)
    if len(prop_match) == 0:
        raise HTTPException(status_code=404, detail=f"Property not found: {bbl}")

    prop = prop_match.row(0, named=True)
    prop_viols = viols.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    pv_score = _compute_property_score(prop_viols)
    pv_likert = _property_likert(pv_score, prop_viols)

    # Simplified owner
    _, score_row, _ = _owner_lookup(prop.get("registration_id"))
    owner_signal = None
    if score_row:
        owner_signal = {
            "rating": score_row.get("likert_label", "Unknown"),
            "rating_level": score_row.get("likert_level"),
            "properties_managed": score_row["num_properties"],
            "total_violations": score_row["total_violations"],
        }

    return {
        "disclaimer": DISCLAIMER,
        "property": {
            "bbl": bbl,
            "address": prop["address"],
            "jurisdiction": prop["jurisdiction"],
            "units_residential": prop.get("units_residential"),
            "year_built": prop.get("year_built"),
        },
        "rating": pv_likert["label"],
        "rating_level": pv_likert["level"],
        "property_score": pv_score,
        "violations_summary": {
            "total": len(prop_viols),
            "critical": len(prop_viols.filter(pl.col("severity_tier") == 1)),
            "open": len(prop_viols.filter(pl.col("status") == "open")),
        },
        "violations": [
            {
                "severity_tier": v["severity_tier"],
                "status": v["status"],
                "inspection_date": v["inspection_date"],
            }
            for v in prop_viols.slice(0, 200).iter_rows(named=True)
        ],
        "owner": owner_signal,
    }


# ===================================================================
# INVESTIGATOR ROUTER  — /investigator/...
# Requires an API key with scope='investigator'.
# Full detail: owner_id, confidence tiers, score breakdowns.
# ===================================================================
investigator_router = APIRouter(prefix="/investigator", tags=["investigator"])


@investigator_router.get("/jurisdictions")
async def list_jurisdictions(
    _scope: Annotated[str, Depends(_require_investigator)],
) -> dict:
    """List all jurisdictions with summary stats.

    Scored jurisdictions include owner counts, harm scores, and confidence
    breakdowns.  Unscored jurisdictions (no owner data) show violation and
    property counts only — useful for cross-referencing addresses and
    violation patterns even without scored landlords.
    """
    scores = _get_scores()
    viols = _get_violations()
    props = _get_properties()

    # --- scored jurisdictions (from owner scores) ---
    scored_stats = (
        scores.group_by("jurisdiction")
        .agg(
            pl.len().alias("owners_scored"),
            pl.col("total_violations").sum().alias("total_violations"),
            pl.col("class_c_violations").sum().alias("critical_violations"),
            pl.col("total_harm_score").mean().alias("avg_score"),
            pl.col("total_harm_score").max().alias("max_score"),
            pl.col("num_properties").sum().alias("total_properties"),
        )
    )
    scored_jurs = set(scored_stats["jurisdiction"].to_list())

    conf = (
        scores.group_by("jurisdiction", "confidence")
        .len()
        .sort("jurisdiction", "confidence")
    )

    # --- all jurisdictions from violation + property data ---
    viol_stats = viols.group_by("jurisdiction").agg(
        pl.len().alias("violation_records"),
        (pl.col("severity_tier") == 1).sum().alias("critical_violation_records"),
        (pl.col("status") == "open").sum().alias("open_violation_records"),
    )
    prop_stats = props.group_by("jurisdiction").agg(
        pl.len().alias("property_records"),
        pl.col("address").is_not_null().sum().alias("addresses_indexed"),
    )
    all_jurs = set(viol_stats["jurisdiction"].to_list()) | set(prop_stats["jurisdiction"].to_list())

    jur_list = []

    # Scored jurisdictions first
    for row in scored_stats.sort("owners_scored", descending=True).iter_rows(named=True):
        jur = row["jurisdiction"]
        jur_conf = conf.filter(pl.col("jurisdiction") == jur)
        conf_dict = {r["confidence"]: r["len"] for r in jur_conf.iter_rows(named=True)}

        # Merge raw violation/property counts
        vr = viol_stats.filter(pl.col("jurisdiction") == jur)
        pr = prop_stats.filter(pl.col("jurisdiction") == jur)

        jur_list.append({
            **row,
            "has_owner_data": True,
            "violation_records": vr["violation_records"][0] if len(vr) else 0,
            "critical_violation_records": vr["critical_violation_records"][0] if len(vr) else 0,
            "open_violation_records": vr["open_violation_records"][0] if len(vr) else 0,
            "property_records": pr["property_records"][0] if len(pr) else 0,
            "addresses_indexed": pr["addresses_indexed"][0] if len(pr) else 0,
            "confidence_breakdown": conf_dict,
        })

    # Unscored jurisdictions
    unscored_jurs = sorted(all_jurs - scored_jurs)
    for jur in unscored_jurs:
        vr = viol_stats.filter(pl.col("jurisdiction") == jur)
        pr = prop_stats.filter(pl.col("jurisdiction") == jur)
        jur_list.append({
            "jurisdiction": jur,
            "owners_scored": 0,
            "total_violations": 0,
            "critical_violations": 0,
            "avg_score": None,
            "max_score": None,
            "total_properties": 0,
            "has_owner_data": False,
            "violation_records": vr["violation_records"][0] if len(vr) else 0,
            "critical_violation_records": vr["critical_violation_records"][0] if len(vr) else 0,
            "open_violation_records": vr["open_violation_records"][0] if len(vr) else 0,
            "property_records": pr["property_records"][0] if len(pr) else 0,
            "addresses_indexed": pr["addresses_indexed"][0] if len(pr) else 0,
            "confidence_breakdown": {},
            "caveat": (
                "This jurisdiction has violation and property data but no "
                "owner/contact records. Address searches and property lookups "
                "work, but no landlord harm scores are available."
            ),
        })

    return {
        "disclaimer": DISCLAIMER,
        "jurisdictions": jur_list,
    }


@investigator_router.get("/jurisdictions/{jurisdiction}/landlords")
async def jurisdiction_landlords(
    jurisdiction: str,
    _scope: Annotated[str, Depends(_require_investigator)],
    min_score: float | None = Query(None, ge=0, description="Minimum harm score"),
    min_properties: int | None = Query(None, ge=1, description="Minimum properties"),
    name: str | None = Query(None, description="Partial owner name (case-insensitive)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    """Paginated, filterable landlord list for a single jurisdiction."""
    df = _get_scores()
    jur_df = df.filter(pl.col("jurisdiction") == jurisdiction)

    if len(jur_df) == 0:
        raise HTTPException(status_code=404, detail=f"No data for jurisdiction: {jurisdiction}")

    if min_score is not None:
        jur_df = jur_df.filter(pl.col("total_harm_score") >= min_score)
    if min_properties is not None:
        jur_df = jur_df.filter(pl.col("num_properties") >= min_properties)
    if name:
        jur_df = jur_df.filter(
            pl.col("owner_id").str.to_uppercase().str.contains(name.upper())
        )

    total = len(jur_df)
    page = jur_df.slice(offset, limit)

    return {
        "disclaimer": DISCLAIMER,
        "jurisdiction": jurisdiction,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": page.to_dicts(),
    }


@investigator_router.get("/address/search")
async def investigator_search_address(
    _scope: Annotated[str, Depends(_require_investigator)],
    q: str = Query(..., min_length=3, description="Address substring (case-insensitive)"),
    jurisdiction: str | None = Query(None, description="Filter by jurisdiction code"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search properties — investigator version with full owner detail."""
    viols = _get_violations()
    total, page = _address_matches(q, jurisdiction, limit, offset)

    results = []
    for row in page.iter_rows(named=True):
        bbl = row["bbl"]
        prop_viols = viols.filter(pl.col("bbl") == bbl)
        pv_score = _compute_property_score(prop_viols)

        owner_id, score_row, reg_info = _owner_lookup(row.get("registration_id"))
        owner_info = None
        if score_row:
            owner_info = {
                "owner_id": owner_id,
                "confidence": reg_info["confidence"] if reg_info else None,
                "total_harm_score": score_row["total_harm_score"],
                "num_properties": score_row["num_properties"],
                "total_violations": score_row["total_violations"],
            }

        results.append({
            "bbl": bbl,
            "address": row["address"],
            "jurisdiction": row["jurisdiction"],
            "units_residential": row.get("units_residential"),
            "year_built": row.get("year_built"),
            "property_score": pv_score["property_score"],
            "violations": len(prop_viols),
            "critical_violations": len(prop_viols.filter(pl.col("severity_tier") == 1)),
            "open_violations": len(prop_viols.filter(pl.col("status") == "open")),
            "owner": owner_info,
        })

    return {
        "disclaimer": DISCLAIMER,
        "query": q,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


@investigator_router.get("/property/{bbl}")
async def investigator_get_property(
    bbl: str,
    _scope: Annotated[str, Depends(_require_investigator)],
) -> dict:
    """Full property detail with owner identity and score breakdown."""
    props = _get_properties()
    viols = _get_violations()

    prop_match = props.filter(pl.col("bbl") == bbl)
    if len(prop_match) == 0:
        raise HTTPException(status_code=404, detail=f"Property not found: {bbl}")

    prop = prop_match.row(0, named=True)
    prop_viols = viols.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    pv_score = _compute_property_score(prop_viols)

    _, score_row, _ = _owner_lookup(prop.get("registration_id"))

    return {
        "disclaimer": DISCLAIMER,
        "property": {
            "bbl": bbl,
            "address": prop["address"],
            "jurisdiction": prop["jurisdiction"],
            "units_residential": prop.get("units_residential"),
            "year_built": prop.get("year_built"),
        },
        "property_score": pv_score,
        "violations_summary": {
            "total": len(prop_viols),
            "critical": len(prop_viols.filter(pl.col("severity_tier") == 1)),
            "open": len(prop_viols.filter(pl.col("status") == "open")),
        },
        "violations": prop_viols.slice(0, 200).to_dicts(),
        "owner": score_row,
    }


@investigator_router.get("/landlords/search")
async def search_landlords(
    _scope: Annotated[str, Depends(_require_investigator)],
    name: str | None = Query(None, description="Partial owner name (case-insensitive)"),
    jurisdiction: str | None = Query(None, description="Filter by jurisdiction code"),
    min_score: float | None = Query(None, ge=0, description="Minimum harm score"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search landlords across all jurisdictions."""
    df = _get_scores()

    if name:
        df = df.filter(pl.col("owner_id").str.to_uppercase().str.contains(name.upper()))
    if jurisdiction:
        df = df.filter(pl.col("jurisdiction") == jurisdiction)
    if min_score is not None:
        df = df.filter(pl.col("total_harm_score") >= min_score)

    total = len(df)
    page = df.slice(offset, limit)

    return {
        "disclaimer": DISCLAIMER,
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": page.to_dicts(),
    }


@investigator_router.get("/landlords/{owner_id}")
async def get_landlord(
    owner_id: str,
    _scope: Annotated[str, Depends(_require_investigator)],
) -> dict:
    """Full harm score detail for a single landlord, including score breakdown."""
    df = _get_scores()
    match = df.filter(pl.col("owner_id") == owner_id)
    if len(match) == 0:
        raise HTTPException(status_code=404, detail=f"Owner not found: {owner_id}")

    row = match.row(0, named=True)

    severity_w = float(row.get("severity_score", 0)) * 0.4
    density_w = float(row.get("density_score", 0)) * 100 * 0.3
    widespread_w = float(row.get("widespread_score", 0)) * 100 * 0.2
    persistence_w = float(row.get("persistence_score", 0)) * 100 * 0.1

    return {
        "disclaimer": DISCLAIMER,
        **row,
        "score_breakdown": {
            "severity_weighted": severity_w,
            "density_weighted": density_w,
            "widespread_weighted": widespread_w,
            "persistence_weighted": persistence_w,
        },
        "confidence_description": {
            "high": "Name + address corroborated — safe for filings",
            "medium": "Name match only — needs corroboration before formal action",
            "low": "Address grouping only — investigative lead",
        }.get(row.get("confidence", ""), ""),
    }


# ---------------------------------------------------------------------------
# App — mount routers + root health check
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Renter Shield API",
    description=(
        "Dual-audience API for housing data.\n\n"
        "- **/renter/** — address-first property lookups for prospective tenants.\n"
        "- **/investigator/** — owner-centric harm-score queries for housing investigations.\n\n"
        "API keys carry a scope (`renter` or `investigator`). Investigator keys "
        "can access all routes; renter keys are restricted to `/renter/` endpoints."
    ),
    version="0.2.0",
)

app.include_router(renter_router)
app.include_router(investigator_router)


@app.middleware("http")
async def _audit_log_middleware(request, call_next):
    """Log authenticated API calls to the audit database."""
    response = await call_next(request)
    # Only log successful authenticated requests (skip health, docs, etc.)
    api_key = request.headers.get("x-api-key")
    if api_key and response.status_code < 400:
        user = audit.validate_token(api_key)
        if user:
            audit.log_api_call(user["id"], request.url.path, request.method)
    return response


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "scores_loaded": _scores_df is not None}
