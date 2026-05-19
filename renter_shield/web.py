"""HTML routes for the renter-facing web UI (htmx + Jinja2).

Serves server-rendered pages behind cookie-based authentication.
Coexists with the JSON API — this module handles browser traffic,
the existing APIRouter handles programmatic ``X-API-Key`` access.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import APIRouter, Cookie, Form, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from renter_shield import audit
from renter_shield.pdf_report import generate_property_report

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
_TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = APIRouter(tags=["web"])

TOKEN_COOKIE = "rs_token"
COOKIE_MAX_AGE = audit.SESSION_EXPIRY_DAYS * 86400  # seconds

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
_OWNER_DATA_JURISDICTIONS = {"nyc", "boston", "philadelphia", "chicago", "pittsburgh"}
_SEVERITY_PTS = {1: 5.0, 2: 2.5, 3: 1.0, 4: 0.0}
_SEVERITY_LABELS = {1: "Critical", 2: "Serious", 3: "Minor", 4: "Info"}
_SEV_BAR_COLORS = {1: "#ff2b2b", 2: "#ff8c00", 3: "#faca2b", 4: "#d5dae5"}


# ---------------------------------------------------------------------------
# Data loading (module-level cache — loaded once at startup)
# ---------------------------------------------------------------------------
_scores_df: pl.DataFrame | None = None
_props_df: pl.DataFrame | None = None
_viols_df: pl.DataFrame | None = None
_owner_reg_df: pl.DataFrame | None = None


def _load_data() -> None:
    """Load Parquet data lazily on first request."""
    global _scores_df, _props_df, _viols_df, _owner_reg_df  # noqa: PLW0603
    if _props_df is not None:
        return  # already loaded

    scores_file = OUTPUT_DIR / "all_landlords_harm_scores.parquet"
    props_file = OUTPUT_DIR / "properties.parquet"
    viols_file = OUTPUT_DIR / "violations.parquet"
    owner_file = OUTPUT_DIR / "owner_registrations.parquet"

    _scores_df = pl.read_parquet(scores_file) if scores_file.exists() else pl.DataFrame()
    _props_df = pl.read_parquet(props_file) if props_file.exists() else pl.DataFrame(
        schema={"bbl": pl.Utf8, "registration_id": pl.Utf8, "units_residential": pl.Float64,
                "year_built": pl.Utf8, "address": pl.Utf8, "jurisdiction": pl.Utf8}
    )
    _viols_df = pl.read_parquet(viols_file) if viols_file.exists() else pl.DataFrame(
        schema={"violation_id": pl.Utf8, "bbl": pl.Utf8, "severity_tier": pl.Int8,
                "status": pl.Utf8, "inspection_date": pl.Date, "jurisdiction": pl.Utf8}
    )
    _owner_reg_df = pl.read_parquet(owner_file) if owner_file.exists() else pl.DataFrame(
        schema={"owner_id": pl.Utf8, "jurisdiction": pl.Utf8, "confidence": pl.Utf8,
                "registration_id": pl.Utf8}
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


def _property_violation_score(prop_viols: pl.DataFrame) -> dict:
    n = len(prop_viols)
    if n == 0:
        return {"property_score": 0.0, "severity_score": 0.0, "open_pct": 0.0,
                "total": 0, "critical": 0, "open": 0}
    sev_sum = sum(_SEVERITY_PTS.get(row["severity_tier"], 0) for row in prop_viols.iter_rows(named=True))
    n_open = len(prop_viols.filter(pl.col("status") == "open"))
    n_crit = len(prop_viols.filter(pl.col("severity_tier") == 1))
    open_pct = n_open / n if n else 0
    score = sev_sum * 0.8 + (open_pct * 100) * 0.2
    return {"property_score": round(score, 1), "severity_score": round(sev_sum, 1),
            "open_pct": round(open_pct, 3), "total": n, "critical": n_crit, "open": n_open}


def _property_likert(pv: dict) -> tuple[int, str, str]:
    """Return (level, label, dot_css_class)."""
    score = pv["property_score"]
    n_crit = pv["critical"]
    n_open = pv["open"]
    n_total = pv["total"]
    open_pct = pv["open_pct"]

    if n_total == 0:
        return 1, "No issues found", "green"
    if n_crit == 0 and n_open == 0 and score < 10:
        return 2, "Minor issues", "green"
    if n_crit == 0 and (n_open > 0 or score >= 10) and score < 50:
        return 3, "Some concerns", "yellow"
    if (n_crit > 0 or score >= 50) and not (n_crit > 0 and open_pct > 0.3):
        return 4, "Significant issues", "orange"
    return 5, "Severe issues", "red"


def _owner_dot_color(likert_label: str) -> str:
    """Map a likert_label to a CSS dot class."""
    label = (likert_label or "").lower()
    if "low" in label:
        return "green"
    if "some" in label:
        return "yellow"
    if "moderate" in label:
        return "orange"
    return "red"


def _no_owner_message(jurisdiction: str) -> str:
    reasons = {
        "sf": "San Francisco does not publish landlord/owner records in its open data.",
        "seattle": "Seattle's open data portal does not include property owner records.",
    }
    if jurisdiction in reasons:
        return f"Owner unknown — {reasons[jurisdiction]}"
    if jurisdiction not in _OWNER_DATA_JURISDICTIONS and not jurisdiction.startswith("hud_reac_"):
        return "Owner unknown — Owner data is not available for this jurisdiction."
    return "Owner unknown — No owner record links to this property. The violation history is still available."


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
def _get_user(rs_token: str | None) -> dict | None:
    """Validate the cookie token, return user dict or None."""
    if not rs_token:
        return None
    return audit.validate_token(rs_token)


def _set_token_cookie(response: Response, token: str) -> Response:
    """Set the auth cookie on a response."""
    response.set_cookie(
        key=TOKEN_COOKIE,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=False,  # Set True when behind HTTPS in prod (nginx handles TLS)
    )
    return response


def _require_auth(rs_token: str | None):
    """Return user dict or a redirect response to registration."""
    user = _get_user(rs_token)
    if not user:
        return None
    return user


# ---------------------------------------------------------------------------
# Registration routes
# ---------------------------------------------------------------------------
@router.get("/renter/register", response_class=HTMLResponse)
async def register_page(request: Request, rs_token: Annotated[str | None, Cookie()] = None):
    """Show registration form, or redirect if already authenticated."""
    user = _get_user(rs_token)
    if user:
        return RedirectResponse("/renter/", status_code=302)
    return templates.TemplateResponse(request, "register.html", {"error": None})


@router.post("/renter/register", response_class=HTMLResponse)
async def register_submit(
    request: Request,
    name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    role: Annotated[str, Form()] = "",
    agree: Annotated[str | None, Form()] = None,
):
    """Process registration form submission."""
    if not agree:
        return templates.TemplateResponse(
            request, "register.html", {"error": "You must agree to the disclaimer to continue."},
        )

    if not name.strip() or not email.strip():
        return templates.TemplateResponse(
            request, "register.html", {"error": "Name and email are required."},
        )

    ip = request.client.host if request.client else ""

    user = audit.register_user(
        name=name,
        email=email,
        role=role,
        scope="renter",
        ip=ip,
    )

    response = templates.TemplateResponse(
        request, "registered.html", {"user": user, "token": user["token"]},
    )
    return _set_token_cookie(response, user["token"])


@router.post("/renter/token-login", response_class=HTMLResponse)
async def token_login(request: Request, token: Annotated[str, Form()]):
    """Sign in with an existing token."""
    user = audit.validate_token(token.strip())
    if not user:
        return templates.TemplateResponse(
            request, "register.html", {"error": "Invalid or expired token."},
        )
    response = RedirectResponse("/renter/", status_code=302)
    return _set_token_cookie(response, user["token"])


# ---------------------------------------------------------------------------
# Renter pages
# ---------------------------------------------------------------------------
@router.get("/renter/", response_class=HTMLResponse)
async def renter_search_page(
    request: Request,
    q: str = "",
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """Main search page — full page render."""
    user = _require_auth(rs_token)
    if not user:
        return RedirectResponse("/renter/register", status_code=302)

    _load_data()
    audit.log_page_view(user["id"], "renter", "address_search")

    ctx: dict = {"user": user, "q": q, "results": []}

    if q and len(q) >= 3:
        ctx["results"] = _do_search(q)

    return templates.TemplateResponse(request, "search.html", ctx)


@router.get("/renter/search", response_class=HTMLResponse)
async def renter_search_fragment(
    request: Request,
    q: Annotated[str, Query(min_length=3)],
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """htmx fragment — search results only (no full page)."""
    user = _require_auth(rs_token)
    if not user:
        return HTMLResponse("<p>Session expired. <a href='/renter/register'>Register again</a>.</p>", status_code=401)

    _load_data()
    results = _do_search(q)
    return templates.TemplateResponse(
        request, "partials/search_results.html", {"results": results, "q": q},
    )


def _do_search(q: str) -> list[dict]:
    """Run address search and return result dicts for the template."""
    matches = _props_df.filter(
        pl.col("address").is_not_null()
        & pl.col("address").str.to_uppercase().str.contains(q.upper())
    ).sort("jurisdiction", "address").head(100)

    results = []
    for row in matches.iter_rows(named=True):
        bbl = row["bbl"]
        prop_viols = _viols_df.filter(pl.col("bbl") == bbl)
        pv_stats = _property_violation_score(prop_viols)
        _, lk_label, dot_color = _property_likert(pv_stats)

        reg_id = row.get("registration_id")
        owner_match = _owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()
        owner_label = "Owner on file" if len(owner_match) > 0 else "Not available"

        results.append({
            "bbl": bbl,
            "address": row["address"] or bbl,
            "city": _display_jurisdiction(row["jurisdiction"]),
            "rating_label": lk_label,
            "dot_color": dot_color,
            "total": pv_stats["total"],
            "critical": pv_stats["critical"],
            "open": pv_stats["open"],
            "owner_label": owner_label,
        })
    return results


@router.get("/renter/property/{bbl}", response_class=HTMLResponse)
async def renter_property_page(
    request: Request,
    bbl: str,
    q: str = "",
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """Property detail page."""
    user = _require_auth(rs_token)
    if not user:
        return RedirectResponse("/renter/register", status_code=302)

    _load_data()
    audit.log_page_view(user["id"], "renter", "property", {"bbl": bbl})

    prop_match = _props_df.filter(pl.col("bbl") == bbl)
    if len(prop_match) == 0:
        return templates.TemplateResponse(
            request, "404.html",
            {"user": user, "message": f"No property found for '{bbl}'."},
            status_code=404,
        )

    prop = prop_match.row(0, named=True)
    jur = prop["jurisdiction"]
    addr = prop["address"] or bbl

    prop_viols = _viols_df.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    pv = _property_violation_score(prop_viols)
    _, rating_label, dot_color = _property_likert(pv)

    units = prop.get("units_residential")
    units_str = f"{units:,.0f}" if units else "Unknown"
    yb = prop.get("year_built") or "Unknown"

    # Severity bar chart data
    severity_counts = []
    if pv["total"] > 0:
        sev_grouped = prop_viols.group_by("severity_tier").len().sort("severity_tier")
        max_count = sev_grouped["len"].max() if len(sev_grouped) > 0 else 1
        for row in sev_grouped.iter_rows(named=True):
            tier = row["severity_tier"]
            count = row["len"]
            severity_counts.append({
                "label": _SEVERITY_LABELS.get(tier, "Unknown"),
                "count": count,
                "pct": round(count / max_count * 100),
                "bar_color": _SEV_BAR_COLORS.get(tier, "#ccc"),
            })

    # Owner info
    owner_ctx = None
    no_owner_msg = ""
    reg_id = prop.get("registration_id")
    owner_match = _owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()

    if len(owner_match) > 0:
        owner_id = owner_match["owner_id"][0]
        score_match = _scores_df.filter(pl.col("owner_id") == owner_id) if len(_scores_df) > 0 else pl.DataFrame()
        if len(score_match) > 0:
            orow = score_match.row(0, named=True)
            display_name = owner_id.split(" [")[0].replace("_", " ").title() if owner_id else "Unknown"
            owner_ctx = {
                "owner_id": owner_id,
                "display_name": display_name,
                "likert_label": orow.get("likert_label", "Unknown"),
                "dot_color": _owner_dot_color(orow.get("likert_label", "")),
                "num_properties": orow["num_properties"],
                "total_violations": orow["total_violations"],
            }
    if not owner_ctx:
        no_owner_msg = _no_owner_message(jur)

    return templates.TemplateResponse(request, "property.html", {
        "user": user,
        "bbl": bbl,
        "address": addr,
        "jurisdiction": _display_jurisdiction(jur),
        "rating_label": rating_label,
        "dot_color": dot_color,
        "units": units_str,
        "year_built": yb,
        "pv": pv,
        "severity_counts": severity_counts,
        "owner": owner_ctx,
        "no_owner_msg": no_owner_msg,
        "back_query": q,
    })


@router.get("/renter/property/{bbl}/violations", response_class=HTMLResponse)
async def renter_violations_fragment(
    request: Request,
    bbl: str,
    page: int = 1,
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """Paginated violations table fragment (htmx)."""
    user = _require_auth(rs_token)
    if not user:
        return HTMLResponse("<p>Session expired.</p>", status_code=401)

    _load_data()
    page_size = 25
    prop_viols = _viols_df.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    n_total = len(prop_viols)
    total_pages = max((n_total + page_size - 1) // page_size, 1)
    page = max(1, min(page, total_pages))

    page_data = prop_viols.slice((page - 1) * page_size, page_size)
    violations = [
        {
            "date": str(r["inspection_date"]) if r["inspection_date"] else "",
            "severity": _SEVERITY_LABELS.get(r["severity_tier"], "Unknown"),
            "status": (r["status"] or "").title(),
        }
        for r in page_data.iter_rows(named=True)
    ]

    return templates.TemplateResponse(request, "partials/violations_page.html", {
        "bbl": bbl,
        "violations": violations,
        "page": page,
        "total_pages": total_pages,
    })


@router.get("/renter/property/{bbl}/report.pdf")
async def renter_pdf_report(
    bbl: str,
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """Generate and return the PDF property report."""
    user = _require_auth(rs_token)
    if not user:
        return RedirectResponse("/renter/register", status_code=302)

    _load_data()

    prop_match = _props_df.filter(pl.col("bbl") == bbl)
    if len(prop_match) == 0:
        return Response("Property not found", status_code=404)

    prop = prop_match.row(0, named=True)
    jur = prop["jurisdiction"]
    addr = prop["address"] or bbl
    jur_display = _display_jurisdiction(jur)

    prop_viols = _viols_df.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    pv = _property_violation_score(prop_viols)
    _, lk_label, dot_color = _property_likert(pv)

    # Emoji for PDF
    _emoji_map = {"green": "🟢", "yellow": "🟡", "orange": "🟠", "red": "🔴"}
    rating_label_pdf = f"{_emoji_map.get(dot_color, '')} {lk_label}"

    units = prop.get("units_residential")
    units_str = f"{units:,.0f}" if units else "Unknown"
    yb = prop.get("year_built") or "Unknown"

    # Owner info for PDF
    pdf_owner_name = None
    pdf_owner_rating = None
    pdf_owner_props = None
    pdf_owner_viols = None

    reg_id = prop.get("registration_id")
    owner_match = _owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()
    if len(owner_match) > 0:
        oid = owner_match["owner_id"][0]
        smatch = _scores_df.filter(pl.col("owner_id") == oid) if len(_scores_df) > 0 else pl.DataFrame()
        if len(smatch) > 0:
            orow = smatch.row(0, named=True)
            pdf_owner_name = oid.split(" [")[0].replace("_", " ").title()
            o_color = _owner_dot_color(orow.get("likert_label", ""))
            pdf_owner_rating = f"{_emoji_map.get(o_color, '')} {orow.get('likert_label', '')}".strip()
            pdf_owner_props = orow["num_properties"]
            pdf_owner_viols = orow["total_violations"]

    viol_rows = [
        {
            "date": str(r["inspection_date"]) if r["inspection_date"] else "",
            "severity": _SEVERITY_LABELS.get(r["severity_tier"], "Unknown"),
            "status": (r["status"] or "").title(),
            "violation_id": r.get("violation_id", ""),
        }
        for r in prop_viols.iter_rows(named=True)
    ]

    pdf_bytes = generate_property_report(
        address=addr,
        jurisdiction=jur_display,
        rating_label=rating_label_pdf,
        units=units_str,
        year_built=yb,
        total_violations=pv["total"],
        critical=pv["critical"],
        open_violations=pv["open"],
        open_pct=pv["open_pct"],
        owner_name=pdf_owner_name,
        owner_rating=pdf_owner_rating,
        owner_properties=pdf_owner_props,
        owner_total_violations=pdf_owner_viols,
        violations=viol_rows,
    )

    safe_name = addr.replace(" ", "_").replace(",", "")[:50]
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="renter_shield_{safe_name}.pdf"'},
    )


@router.get("/renter/owner/{owner_id:path}", response_class=HTMLResponse)
async def renter_owner_page(
    request: Request,
    owner_id: str,
    rs_token: Annotated[str | None, Cookie()] = None,
):
    """Owner detail page (simplified for renters)."""
    user = _require_auth(rs_token)
    if not user:
        return RedirectResponse("/renter/register", status_code=302)

    _load_data()
    audit.log_page_view(user["id"], "renter", "owner", {"owner": owner_id})

    score_match = _scores_df.filter(pl.col("owner_id") == owner_id) if len(_scores_df) > 0 else pl.DataFrame()
    if len(score_match) == 0:
        return templates.TemplateResponse(
            request, "404.html",
            {"user": user, "message": "Owner not found or no scored data available."},
            status_code=404,
        )

    orow = score_match.row(0, named=True)
    jur = orow.get("jurisdiction", "")
    display_name = owner_id.split(" [")[0].replace("_", " ").title() if owner_id else "Unknown"
    likert_label = orow.get("likert_label", "Unknown")
    dot_color = _owner_dot_color(likert_label)

    # Properties owned
    owner_regs = _owner_reg_df.filter(pl.col("owner_id") == owner_id)
    properties = []
    if len(owner_regs) > 0:
        reg_ids = owner_regs["registration_id"].to_list()
        owner_props = _props_df.filter(pl.col("registration_id").is_in(reg_ids))
        for row in owner_props.sort("address").iter_rows(named=True):
            prop_bbl = row["bbl"]
            pviols = _viols_df.filter(pl.col("bbl") == prop_bbl)
            pv_stats = _property_violation_score(pviols)
            _, p_label, p_dot = _property_likert(pv_stats)
            properties.append({
                "bbl": prop_bbl,
                "address": row["address"] or prop_bbl,
                "rating_label": p_label,
                "dot_color": p_dot,
                "total": pv_stats["total"],
                "critical": pv_stats["critical"],
                "open": pv_stats["open"],
            })

    return templates.TemplateResponse(request, "owner.html", {
        "user": user,
        "owner_id": owner_id,
        "display_name": display_name,
        "jurisdiction": _display_jurisdiction(jur),
        "likert_label": likert_label,
        "dot_color": dot_color,
        "num_properties": orow["num_properties"],
        "total_violations": orow["total_violations"],
        "properties": properties,
    })


@router.get("/renter/sign-out")
async def renter_sign_out():
    """Clear the auth cookie and redirect to registration page."""
    response = RedirectResponse("/renter/register", status_code=302)
    response.delete_cookie(key=TOKEN_COOKIE)
    return response
