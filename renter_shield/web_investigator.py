"""HTML routes for the investigator-facing web UI (htmx + Jinja2).

Owner-centric harm-score explorer for housing investigations.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import APIRouter, Cookie, Form, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from renter_shield import audit

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
_TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = APIRouter(tags=["web-investigator"])

TOKEN_COOKIE = "rs_token"
COOKIE_MAX_AGE = audit.SESSION_EXPIRY_DAYS * 86400

OUTPUT_DIR = Path(os.getenv("LI_OUTPUT_DIR", "output"))

# Jurisdiction display names
_JURISDICTION_DISPLAY = {
    "nyc": "New York City",
    "boston": "Boston",
    "philadelphia": "Philadelphia",
    "chicago": "Chicago",
    "sf": "San Francisco",
    "seattle": "Seattle",
    "pittsburgh": "Pittsburgh",
    "la": "Los Angeles",
    "austin": "Austin",
    "miami": "Miami-Dade",
    "detroit": "Detroit",
}
_US_STATES = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas",
    "ca": "California", "co": "Colorado", "ct": "Connecticut", "de": "Delaware",
    "dc": "D.C.", "fl": "Florida", "ga": "Georgia", "hi": "Hawaii",
    "id": "Idaho", "il": "Illinois", "in": "Indiana", "ia": "Iowa",
    "ks": "Kansas", "ky": "Kentucky", "la": "Louisiana", "me": "Maine",
    "md": "Maryland", "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota",
    "ms": "Mississippi", "mo": "Missouri", "mt": "Montana", "ne": "Nebraska",
    "nv": "Nevada", "nh": "New Hampshire", "nj": "New Jersey", "nm": "New Mexico",
    "ny": "New York", "nc": "North Carolina", "nd": "North Dakota", "oh": "Ohio",
    "ok": "Oklahoma", "or": "Oregon", "pa": "Pennsylvania", "ri": "Rhode Island",
    "sc": "South Carolina", "sd": "South Dakota", "tn": "Tennessee", "tx": "Texas",
    "ut": "Utah", "vt": "Vermont", "va": "Virginia", "wa": "Washington",
    "wv": "West Virginia", "wi": "Wisconsin", "wy": "Wyoming",
    "pr": "Puerto Rico", "vi": "U.S. Virgin Islands", "gu": "Guam",
    "as": "American Samoa", "mp": "Northern Mariana Islands",
}
_CONFIDENCE_LABELS = {"high": "🟢 High", "medium": "🟡 Medium", "low": "🟠 Low"}
_CONFIDENCE_EMOJIS = {"high": "🟢", "medium": "🟡", "low": "🟠"}
_CONFIDENCE_DESCS = {
    "high": "Name + address corroborated — safe for filings",
    "medium": "Name match only — needs corroboration",
    "low": "Address grouping only — investigative lead",
}

# ---------------------------------------------------------------------------
# Data loading (module-level cache)
# ---------------------------------------------------------------------------
_scores_df: pl.DataFrame | None = None
_props_df: pl.DataFrame | None = None
_viols_df: pl.DataFrame | None = None


def _load_data() -> None:
    global _scores_df, _props_df, _viols_df  # noqa: PLW0603
    if _scores_df is not None:
        return

    scores_file = OUTPUT_DIR / "all_landlords_harm_scores.parquet"
    props_file = OUTPUT_DIR / "properties.parquet"
    viols_file = OUTPUT_DIR / "violations.parquet"

    _scores_df = pl.read_parquet(scores_file) if scores_file.exists() else pl.DataFrame()
    _props_df = pl.read_parquet(props_file) if props_file.exists() else pl.DataFrame(
        schema={"bbl": pl.Utf8, "address": pl.Utf8, "jurisdiction": pl.Utf8,
                "units_residential": pl.Float64}
    )
    _viols_df = pl.read_parquet(viols_file) if viols_file.exists() else pl.DataFrame(
        schema={"violation_id": pl.Utf8, "bbl": pl.Utf8, "severity_tier": pl.Int8,
                "status": pl.Utf8, "jurisdiction": pl.Utf8}
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _display_jurisdiction(jur: str) -> str:
    if jur in _JURISDICTION_DISPLAY:
        return _JURISDICTION_DISPLAY[jur]
    if jur.startswith("hud_reac_"):
        state_code = jur[len("hud_reac_"):]
        return f"HUD REAC — {_US_STATES.get(state_code, state_code.upper())}"
    return jur.replace("_", " ").title()


def _get_user(rs_token: str | None) -> dict | None:
    if not rs_token:
        return None
    return audit.validate_token(rs_token)


def _require_investigator(rs_token: str | None):
    """Return user dict if investigator-scoped, else None."""
    user = _get_user(rs_token)
    if not user:
        return None
    if user.get("scope") != "investigator":
        return None
    return user


def _set_token_cookie(response: Response, token: str) -> Response:
    response.set_cookie(
        key=TOKEN_COOKIE, value=token, max_age=COOKIE_MAX_AGE,
        httponly=True, samesite="lax", secure=False,
    )
    return response


def _owner_dot_color(likert_label: str | None) -> str:
    label = (likert_label or "").lower()
    if "low" in label:
        return "green"
    if "some" in label:
        return "yellow"
    if "moderate" in label:
        return "orange"
    return "red"


_CONF_ORDER = ["high", "medium", "low"]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
@router.get("/investigator/register", response_class=HTMLResponse)
async def inv_register_page(request: Request, rs_token: Annotated[str | None, Cookie()] = None):
    user = _get_user(rs_token)
    if user and user.get("scope") == "investigator":
        return RedirectResponse("/investigator/", status_code=302)
    return templates.TemplateResponse(request, "inv_register.html", {"error": None})


@router.post("/investigator/register", response_class=HTMLResponse)
async def inv_register_submit(
    request: Request,
    name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    role: Annotated[str, Form()] = "",
    agree: Annotated[str | None, Form()] = None,
):
    if not agree:
        return templates.TemplateResponse(
            request, "inv_register.html",
            {"error": "You must agree to the disclaimer to continue."},
        )
    if not name.strip() or not email.strip():
        return templates.TemplateResponse(
            request, "inv_register.html",
            {"error": "Name and email are required."},
        )
    ip = request.client.host if request.client else ""
    user = audit.register_user(name=name, email=email, role=role, scope="investigator", ip=ip)
    response = templates.TemplateResponse(
        request, "inv_registered.html", {"user": user, "token": user["token"]},
    )
    return _set_token_cookie(response, user["token"])


@router.post("/investigator/token-login", response_class=HTMLResponse)
async def inv_token_login(request: Request, token: Annotated[str, Form()]):
    user = audit.validate_token(token.strip())
    if not user or user.get("scope") != "investigator":
        return templates.TemplateResponse(
            request, "inv_register.html",
            {"error": "Invalid, expired, or non-investigator token."},
        )
    response = RedirectResponse("/investigator/", status_code=302)
    return _set_token_cookie(response, user["token"])


@router.get("/investigator/sign-out")
async def inv_sign_out():
    response = RedirectResponse("/investigator/register", status_code=302)
    response.delete_cookie(TOKEN_COOKIE)
    return response


# ---------------------------------------------------------------------------
# Overview page
# ---------------------------------------------------------------------------
@router.get("/investigator/", response_class=HTMLResponse)
async def inv_overview(request: Request, rs_token: Annotated[str | None, Cookie()] = None):
    user = _require_investigator(rs_token)
    if not user:
        return RedirectResponse("/investigator/register", status_code=302)

    _load_data()
    audit.log_page_view(user["id"], "investigator", "overview")

    scores = _scores_df
    props = _props_df
    viols = _viols_df

    # Scored jurisdiction stats
    jur_stats = (
        scores.group_by("jurisdiction")
        .agg(
            pl.len().alias("owners"),
            pl.col("total_violations").sum().alias("violations"),
            pl.col("class_c_violations").sum().alias("critical"),
            pl.col("total_harm_score").mean().alias("avg_score"),
            pl.col("total_harm_score").max().alias("max_score"),
            pl.col("num_properties").sum().alias("properties"),
        )
        .sort("owners", descending=True)
    ) if len(scores) > 0 else pl.DataFrame()

    scored_jurs = set(jur_stats["jurisdiction"].to_list()) if len(jur_stats) > 0 else set()

    scored_jurisdictions = []
    for row in jur_stats.iter_rows(named=True):
        scored_jurisdictions.append({
            "code": row["jurisdiction"],
            "display": _display_jurisdiction(row["jurisdiction"]),
            "owners": row["owners"],
            "violations": row["violations"],
            "critical": row["critical"],
            "avg_score": row["avg_score"] or 0,
        })

    # Unscored jurisdictions
    all_jurs: set[str] = set()
    if len(viols) > 0:
        all_jurs |= set(viols["jurisdiction"].unique().to_list())
    if len(props) > 0:
        all_jurs |= set(props["jurisdiction"].unique().to_list())
    unscored_jurs = sorted(all_jurs - scored_jurs)

    unscored_jurisdictions = []
    for jur in unscored_jurs:
        jv = viols.filter(pl.col("jurisdiction") == jur) if len(viols) > 0 else pl.DataFrame()
        jp = props.filter(pl.col("jurisdiction") == jur) if len(props) > 0 else pl.DataFrame()
        n_viols = len(jv)
        n_crit = len(jv.filter(pl.col("severity_tier") == 1)) if n_viols else 0
        unscored_jurisdictions.append({
            "code": jur,
            "display": _display_jurisdiction(jur),
            "violations": n_viols,
            "critical": n_crit,
            "properties": len(jp),
        })

    # Confidence counts
    confidence_counts = []
    if len(scores) > 0:
        conf_df = (
            scores.group_by("confidence").len()
            .with_columns(
                pl.col("confidence").replace_strict(
                    {c: i for i, c in enumerate(_CONF_ORDER)}, default=99
                ).alias("_sort")
            )
            .sort("_sort").drop("_sort")
        )
        for row in conf_df.iter_rows(named=True):
            confidence_counts.append({
                "level": row["confidence"],
                "emoji": _CONFIDENCE_EMOJIS.get(row["confidence"], ""),
                "count": row["len"],
            })

    stats = {
        "jurisdictions": len(scored_jurs) + len(unscored_jurs),
        "scored_owners": len(scores),
        "total_violations": int(scores["total_violations"].sum()) if len(scores) > 0 else 0,
    }

    return templates.TemplateResponse(request, "inv_overview.html", {
        "user": user,
        "stats": type("S", (), stats)(),
        "confidence_counts": confidence_counts,
        "scored_jurisdictions": scored_jurisdictions,
        "unscored_jurisdictions": unscored_jurisdictions,
    })


# ---------------------------------------------------------------------------
# Cross-jurisdiction search (htmx fragment)
# ---------------------------------------------------------------------------
@router.get("/investigator/search", response_class=HTMLResponse)
async def inv_search_fragment(
    request: Request,
    q: Annotated[str, Query(min_length=3)],
    rs_token: Annotated[str | None, Cookie()] = None,
):
    user = _require_investigator(rs_token)
    if not user:
        return HTMLResponse("<p>Session expired. <a href='/investigator/register'>Register again</a>.</p>", status_code=401)

    _load_data()
    results = []
    if len(_scores_df) > 0:
        matches = _scores_df.filter(
            pl.col("owner_id").str.to_uppercase().str.contains(q.upper())
        ).sort("total_harm_score", descending=True).head(100)

        for row in matches.iter_rows(named=True):
            results.append({
                "display_name": row["owner_id"],
                "owner_id_encoded": urllib.parse.quote(row["owner_id"], safe=""),
                "display_jur": _display_jurisdiction(row["jurisdiction"]),
                "harm_score": row["total_harm_score"],
                "num_properties": row["num_properties"],
                "total_violations": row["total_violations"],
            })

    return templates.TemplateResponse(
        request, "partials/inv_search_results.html",
        {"results": results, "query": q},
    )


# ---------------------------------------------------------------------------
# Jurisdiction page
# ---------------------------------------------------------------------------
@router.get("/investigator/jurisdiction/{jur_code}", response_class=HTMLResponse)
async def inv_jurisdiction_page(
    request: Request,
    jur_code: str,
    rs_token: Annotated[str | None, Cookie()] = None,
):
    user = _require_investigator(rs_token)
    if not user:
        return RedirectResponse("/investigator/register", status_code=302)

    _load_data()
    audit.log_page_view(user["id"], "investigator", "jurisdiction", {"jur": jur_code})

    scores = _scores_df
    jur_df = scores.filter(pl.col("jurisdiction") == jur_code) if len(scores) > 0 else pl.DataFrame()
    is_unscored = len(jur_df) == 0

    display_jur = _display_jurisdiction(jur_code)

    if is_unscored:
        # Stats from raw data
        jv = _viols_df.filter(pl.col("jurisdiction") == jur_code) if len(_viols_df) > 0 else pl.DataFrame()
        jp = _props_df.filter(pl.col("jurisdiction") == jur_code) if len(_props_df) > 0 else pl.DataFrame()
        n_viols = len(jv)
        stats = {
            "properties": len(jp),
            "violations": n_viols,
            "critical": len(jv.filter(pl.col("severity_tier") == 1)) if n_viols else 0,
            "open": len(jv.filter(pl.col("status") == "open")) if n_viols else 0,
        }
        return templates.TemplateResponse(request, "inv_jurisdiction.html", {
            "user": user,
            "display_jur": display_jur,
            "jur_code": jur_code,
            "is_unscored": True,
            "stats": type("S", (), stats)(),
            "confidence_counts": [],
            "filters": {},
        })

    # Scored jurisdiction
    stats = {
        "owners": len(jur_df),
        "violations": int(jur_df["total_violations"].sum()),
        "critical": int(jur_df["class_c_violations"].sum()),
        "avg_score": float(jur_df["total_harm_score"].mean()),
        "max_score": float(jur_df["total_harm_score"].max()),
    }

    # Confidence breakdown
    confidence_counts = []
    conf_df = (
        jur_df.group_by("confidence").len()
        .with_columns(
            pl.col("confidence").replace_strict(
                {c: i for i, c in enumerate(_CONF_ORDER)}, default=99
            ).alias("_sort")
        )
        .sort("_sort").drop("_sort")
    )
    for row in conf_df.iter_rows(named=True):
        confidence_counts.append({
            "level": row["confidence"],
            "emoji": _CONFIDENCE_EMOJIS.get(row["confidence"], ""),
            "count": row["len"],
        })

    # Default filter: min 3 properties
    filters = {"min_score": 0, "min_props": 3, "name": ""}

    # Build the initial owners table with default filters
    filtered = jur_df.filter(pl.col("num_properties") >= 3)
    filtered = filtered.sort("total_harm_score", descending=True)

    owners_ctx = _build_owners_table_ctx(filtered, jur_code, page=1, filters=filters)

    return templates.TemplateResponse(request, "inv_jurisdiction.html", {
        "user": user,
        "display_jur": display_jur,
        "jur_code": jur_code,
        "is_unscored": False,
        "stats": type("S", (), stats)(),
        "confidence_counts": confidence_counts,
        "filters": filters,
        **owners_ctx,
    })


# ---------------------------------------------------------------------------
# Owners table fragment (htmx, paginated + filtered)
# ---------------------------------------------------------------------------
@router.get("/investigator/jurisdiction/{jur_code}/owners", response_class=HTMLResponse)
async def inv_owners_table_fragment(
    request: Request,
    jur_code: str,
    page: int = 1,
    min_score: float = 0,
    min_props: int = 3,
    name: str = "",
    rs_token: Annotated[str | None, Cookie()] = None,
):
    user = _require_investigator(rs_token)
    if not user:
        return HTMLResponse("<p>Session expired.</p>", status_code=401)

    _load_data()
    scores = _scores_df
    jur_df = scores.filter(pl.col("jurisdiction") == jur_code) if len(scores) > 0 else pl.DataFrame()

    # Apply filters
    filtered = jur_df.filter(
        (pl.col("total_harm_score") >= min_score)
        & (pl.col("num_properties") >= min_props)
    )
    if name:
        filtered = filtered.filter(
            pl.col("owner_id").str.to_uppercase().str.contains(name.upper())
        )
    filtered = filtered.sort("total_harm_score", descending=True)

    filters = {"min_score": min_score, "min_props": min_props, "name": name}
    ctx = _build_owners_table_ctx(filtered, jur_code, page=page, filters=filters)

    return templates.TemplateResponse(request, "partials/inv_owners_table.html", ctx)


def _build_owners_table_ctx(
    filtered: pl.DataFrame, jur_code: str, page: int, filters: dict
) -> dict:
    """Build template context for the owners table partial."""
    page_size = 25
    total = len(filtered)
    total_pages = max((total + page_size - 1) // page_size, 1)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size

    page_df = filtered.slice(start, page_size)

    owners = []
    for row in page_df.iter_rows(named=True):
        owners.append({
            "owner_id_encoded": urllib.parse.quote(row["owner_id"], safe=""),
            "display_name": row["owner_id"],
            "confidence": row.get("confidence", ""),
            "confidence_emoji": _CONFIDENCE_EMOJIS.get(row.get("confidence", ""), ""),
            "likert_label": row.get("likert_label", "") or "",
            "dot_color": _owner_dot_color(row.get("likert_label")),
            "harm_score": row["total_harm_score"],
            "num_properties": row["num_properties"],
            "total_violations": row["total_violations"],
            "critical": row.get("class_c_violations", 0),
            "open": row.get("unresolved_violations", 0),
        })

    return {
        "owners": owners,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "jur_code": jur_code,
        "min_score": filters.get("min_score", 0),
        "min_props": filters.get("min_props", 3),
        "name": filters.get("name", ""),
    }


# ---------------------------------------------------------------------------
# Owner detail page
# ---------------------------------------------------------------------------
@router.get("/investigator/owner/{owner_id:path}", response_class=HTMLResponse)
async def inv_owner_detail(
    request: Request,
    owner_id: str,
    rs_token: Annotated[str | None, Cookie()] = None,
):
    user = _require_investigator(rs_token)
    if not user:
        return RedirectResponse("/investigator/register", status_code=302)

    _load_data()

    # URL-decode the owner_id
    owner_id_decoded = urllib.parse.unquote(owner_id)
    audit.log_page_view(user["id"], "investigator", "owner", {"owner": owner_id_decoded})

    scores = _scores_df
    match = scores.filter(pl.col("owner_id") == owner_id_decoded) if len(scores) > 0 else pl.DataFrame()

    if len(match) == 0:
        return templates.TemplateResponse(
            request, "404.html",
            {"user": user, "message": f"Owner not found: {owner_id_decoded}"},
            status_code=404,
        )

    row = match.row(0, named=True)
    jur = row["jurisdiction"]

    # Score breakdown
    sev = float(row.get("severity_score", 0))
    dens = float(row.get("density_score", 0))
    wide = float(row.get("widespread_score", 0))
    pers = float(row.get("persistence_score", 0))

    breakdown = [
        {"component": "Severity", "raw": f"{sev:,.1f}", "weight": "40%", "weighted": f"{sev * 0.4:,.1f}"},
        {"component": "Density (viols/unit)", "raw": f"{dens:.2f}", "weight": "30%", "weighted": f"{dens * 100 * 0.3:,.1f}"},
        {"component": "Widespread (%)", "raw": f"{wide:.1%}", "weight": "20%", "weighted": f"{wide * 100 * 0.2:,.1f}"},
        {"component": "Persistence (%)", "raw": f"{pers:.1%}", "weight": "10%", "weighted": f"{pers * 100 * 0.1:,.1f}"},
    ]

    # Raw JSON
    raw_json = json.dumps(
        {k: (float(v) if isinstance(v, (int, float)) else str(v)) for k, v in row.items()},
        indent=2,
    )

    conf = row.get("confidence", "")

    return templates.TemplateResponse(request, "inv_owner.html", {
        "user": user,
        "display_name": row["owner_id"],
        "display_jur": _display_jurisdiction(jur),
        "jur_code": jur,
        "confidence": conf,
        "confidence_emoji": _CONFIDENCE_EMOJIS.get(conf, ""),
        "confidence_desc": _CONFIDENCE_DESCS.get(conf, ""),
        "harm_score": row["total_harm_score"],
        "svi_composite": row.get("svi_composite"),
        "likert_label": row.get("likert_label", "") or "",
        "dot_color": _owner_dot_color(row.get("likert_label")),
        "num_properties": row["num_properties"],
        "total_violations": row["total_violations"],
        "critical": row.get("class_c_violations", 0),
        "unresolved": row.get("unresolved_violations", 0),
        "total_units": row.get("total_units", 0) or 0,
        "theme_severity": row.get("theme_severity"),
        "theme_portfolio": row.get("theme_portfolio"),
        "theme_compliance": row.get("theme_compliance"),
        "breakdown": breakdown,
        "raw_json": raw_json,
    })
