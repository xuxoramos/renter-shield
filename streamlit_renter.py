"""Streamlit dashboard — Renter-facing address lookup.

Pages:
  1. Address Search (default) — search by address, see violation scores
  2. Property Detail — violations history + simplified owner signal

Run with:
    streamlit run streamlit_renter.py

Expects Parquet files in ``output/`` (produced by the pipeline).
"""

import os
from pathlib import Path

import polars as pl
import streamlit as st

from renter_shield.audit import require_registration, log_page_view
from renter_shield.pdf_report import generate_property_report

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(os.getenv("LI_OUTPUT_DIR", "output"))
SCORES_FILE = OUTPUT_DIR / "all_landlords_harm_scores.parquet"
PROPERTIES_FILE = OUTPUT_DIR / "properties.parquet"
VIOLATIONS_FILE = OUTPUT_DIR / "violations.parquet"
OWNER_REG_FILE = OUTPUT_DIR / "owner_registrations.parquet"

DISCLAIMER = (
    "**Disclaimer:** Scores are derived from publicly available government "
    "records and algorithmic analysis. They do not constitute legal findings, "
    "may not reflect current conditions, and may contain errors due to "
    "name-based ownership resolution. Independent verification is required "
    "before any legal, administrative, or public action."
)

JURISDICTION_DISPLAY = {
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


def _display_jurisdiction(jur: str) -> str:
    if jur in JURISDICTION_DISPLAY:
        return JURISDICTION_DISPLAY[jur]
    if jur.startswith("hud_reac_"):
        state_code = jur[len("hud_reac_"):]
        state_name = _US_STATES.get(state_code, state_code.upper())
        return f"HUD REAC \u2014 {state_name}"
    return jur.replace("_", " ").title()


# Jurisdictions where we have owner/contact data for scoring
_OWNER_DATA_JURISDICTIONS = {"nyc", "boston", "philadelphia", "chicago", "pittsburgh"}

# Same severity weights as the DuckDB scoring engine (config.py)
_SEVERITY_PTS = {1: 5.0, 2: 2.5, 3: 1.0, 4: 0.0}

SEVERITY_LABELS = {1: "Critical", 2: "Serious", 3: "Minor", 4: "Info"}
SEVERITY_COLORS = {1: "🔴", 2: "🟠", 3: "🟡", 4: "⚪"}

# Property-level Likert — absolute thresholds based on violation
# characteristics (not percentiles, because properties are compared
# by renters who need a stable, intuitive scale).
_PROPERTY_LIKERT = [
    # (predicate_fn, level, label, color)
    # Evaluated in order; first match wins.
]


def _property_likert(pv: dict) -> tuple[int, str, str]:
    """Map a property violation summary to a 1-5 Likert rating.

    Returns (level, label, color).
    """
    score = pv["property_score"]
    n_crit = pv["critical"]
    n_open = pv["open"]
    n_total = pv["total"]
    open_pct = pv["open_pct"]

    if n_total == 0:
        return 1, "No issues found", "🟢"
    if n_crit == 0 and n_open == 0 and score < 10:
        return 2, "Minor issues", "🟢"
    if n_crit == 0 and (n_open > 0 or score >= 10) and score < 50:
        return 3, "Some concerns", "🟡"
    if (n_crit > 0 or score >= 50) and not (n_crit > 0 and open_pct > 0.3):
        return 4, "Significant issues", "🟠"
    return 5, "Severe issues", "🔴"


def _property_violation_score(prop_viols: pl.DataFrame) -> dict:
    n = len(prop_viols)
    if n == 0:
        return {
            "property_score": 0.0, "severity_score": 0.0, "open_pct": 0.0,
            "total": 0, "critical": 0, "open": 0,
        }
    sev_sum = sum(
        _SEVERITY_PTS.get(row["severity_tier"], 0)
        for row in prop_viols.iter_rows(named=True)
    )
    n_open = len(prop_viols.filter(pl.col("status") == "open"))
    n_crit = len(prop_viols.filter(pl.col("severity_tier") == 1))
    open_pct = n_open / n if n else 0
    score = sev_sum * 0.8 + (open_pct * 100) * 0.2
    return {
        "property_score": round(score, 1), "severity_score": round(sev_sum, 1),
        "open_pct": round(open_pct, 3), "total": n, "critical": n_crit, "open": n_open,
    }


def _no_owner_message(jurisdiction: str) -> str:
    reasons = {
        "sf": "San Francisco does not publish landlord/owner records in its open data.",
        "seattle": "Seattle\u2019s open data portal does not include property owner records (King County Assessor manages that separately).",
    }
    if jurisdiction in reasons:
        return f"\u2139\ufe0f **Owner unknown** \u2014 {reasons[jurisdiction]}"
    if jurisdiction not in _OWNER_DATA_JURISDICTIONS and not jurisdiction.startswith("hud_reac_"):
        return "\u2139\ufe0f **Owner unknown** \u2014 Owner data is not available for this jurisdiction."
    return "\u2139\ufe0f **Owner unknown** \u2014 No owner record links to this property. The violation history below is still available."


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, max_entries=1)
def load_scores() -> pl.DataFrame:
    if not SCORES_FILE.exists():
        return pl.DataFrame()
    return pl.read_parquet(SCORES_FILE).with_columns(
        pl.col("severity_score").cast(pl.Float64),
    )


@st.cache_data(ttl=3600, max_entries=1)
def load_properties() -> pl.DataFrame:
    if not PROPERTIES_FILE.exists():
        return pl.DataFrame(schema={
            "bbl": pl.Utf8, "registration_id": pl.Utf8,
            "units_residential": pl.Float64, "year_built": pl.Utf8,
            "address": pl.Utf8, "jurisdiction": pl.Utf8,
        })
    return pl.read_parquet(PROPERTIES_FILE)


@st.cache_data(ttl=3600, max_entries=1)
def load_violations() -> pl.DataFrame:
    if not VIOLATIONS_FILE.exists():
        return pl.DataFrame(schema={
            "violation_id": pl.Utf8, "bbl": pl.Utf8,
            "severity_tier": pl.Int8, "status": pl.Utf8,
            "inspection_date": pl.Date, "jurisdiction": pl.Utf8,
        })
    return pl.read_parquet(VIOLATIONS_FILE)


@st.cache_data(ttl=3600, max_entries=1)
def load_owner_registrations() -> pl.DataFrame:
    if not OWNER_REG_FILE.exists():
        return pl.DataFrame(schema={
            "owner_id": pl.Utf8, "jurisdiction": pl.Utf8,
            "confidence": pl.Utf8, "registration_id": pl.Utf8,
        })
    return pl.read_parquet(OWNER_REG_FILE)


# ---------------------------------------------------------------------------
# Page config + data
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Address Lookup — Housing Violations",
    page_icon="🏠",
    layout="wide",
)

df = load_scores()
props_df = load_properties()
viols_df = load_violations()
owner_reg_df = load_owner_registrations()

# ---------------------------------------------------------------------------
# Registration gate — must register before seeing any data
# ---------------------------------------------------------------------------
_audit_user = require_registration("renter")
if _audit_user is None:
    st.stop()

# ---------------------------------------------------------------------------
# Know Your Rights — persistent sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("📋 Know Your Rights")
    st.markdown(
        "Every tenant deserves safe housing. If you're having problems "
        "with your landlord, these resources can help."
    )

    st.subheader("Report a Problem")
    st.markdown(
        "- 🗽 **NYC**: Call 311 or [file online](https://portal.311.nyc.gov/)\n"
        "- 🏛️ **NYOAG**: [File a housing complaint](https://ag.ny.gov/consumer-frauds-bureau/housing)\n"
        "- 📞 **HUD**: [Complaint hotline](https://www.hud.gov/topics/housing_discrimination) — 1-800-669-9777\n"
    )

    st.subheader("Tenant Rights by City")
    with st.expander("New York City"):
        st.markdown(
            "- [Tenant protection laws (HPD)](https://www.nyc.gov/site/hpd/renters/tenant-rights.page)\n"
            "- [Right to Counsel (free attorney)](https://www.righttocounselnyc.org/)\n"
            "- [Met Council on Housing hotline](https://www.metcouncilonhousing.org/) — 212-979-0611\n"
        )
    with st.expander("Boston"):
        st.markdown(
            "- [Boston Inspectional Services](https://www.boston.gov/departments/inspectional-services)\n"
            "- [City of Boston tenant rights](https://www.boston.gov/housing/tenant-rights)\n"
            "- [Greater Boston Legal Services](https://www.gbls.org/)\n"
        )
    with st.expander("Philadelphia"):
        st.markdown(
            "- [Philly Tenant Hotline](https://www.phillytenant.org/) — 267-443-2500\n"
            "- [Community Legal Services](https://clsphila.org/)\n"
            "- [L&I complaints](https://www.phila.gov/departments/department-of-licenses-and-inspections/)\n"
        )
    with st.expander("Chicago"):
        st.markdown(
            "- [Chicago RLTO (tenant ordinance)](https://www.chicago.gov/city/en/depts/doh/provdrs/renters/svcs/rents-right.html)\n"
            "- [Metropolitan Tenants Organization](https://www.tenants-rights.org/)\n"
            "- [LAF (Legal Aid)](https://www.lafchicago.org/)\n"
        )
    with st.expander("Other cities"):
        st.markdown(
            "- [HUD tenant rights (national)](https://www.hud.gov/topics/rental_assistance/tenantrights)\n"
            "- [LawHelp.org — find free legal aid](https://www.lawhelp.org/)\n"
            "- [National Housing Law Project](https://www.nhlp.org/)\n"
        )

    st.divider()
    st.caption(
        "**About Renter Shield** — A project of the New York Office of the "
        "Attorney General. "
        "[Source code](https://github.com/xuxoramos/renter-shield) · "
        "[Dataset](https://doi.org/10.5281/zenodo.19418743)"
    )

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
page = st.query_params.get("page", "address")
nav_bbl = st.query_params.get("bbl", None)
nav_owner = st.query_params.get("owner", None)


def nav_link_address(keep_query: bool = False) -> None:
    st.query_params["page"] = "address"
    if "bbl" in st.query_params:
        del st.query_params["bbl"]
    if not keep_query and "q" in st.query_params:
        del st.query_params["q"]


def nav_link_back_to_results() -> None:
    """Return to the address search page, preserving the search query."""
    st.query_params["page"] = "address"
    if "bbl" in st.query_params:
        del st.query_params["bbl"]


def nav_link_property(bbl: str) -> None:
    st.query_params["page"] = "property"
    st.query_params["bbl"] = bbl
    if "owner" in st.query_params:
        del st.query_params["owner"]


def nav_link_owner(owner_id: str) -> None:
    st.query_params["page"] = "owner"
    st.query_params["owner"] = owner_id
    if "bbl" in st.query_params:
        del st.query_params["bbl"]


# =========================================================================
# PAGE: Address Search
# =========================================================================
def page_address_search() -> None:
    st.title("🏠 Look Up an Address")
    st.caption(DISCLAIMER)

    st.markdown(
        "**Thinking about renting a place?** Enter the address to see "
        "housing code violations and the landlord's track record."
    )

    # Restore search query from URL if returning from property detail
    saved_q = st.query_params.get("q", "")
    search_addr = st.text_input(
        "Address",
        value=saved_q,
        placeholder="e.g. 351 92 STREET, 400 N REDFIELD, 3601 5TH AVE",
        key="addr_search",
    )
    # Persist current query to URL so back-navigation restores it
    if search_addr and len(search_addr) >= 3:
        st.query_params["q"] = search_addr
    elif "q" in st.query_params:
        del st.query_params["q"]

    if not search_addr or len(search_addr) < 3:
        st.info("Enter at least 3 characters to search.")
        return

    matches = props_df.filter(
        pl.col("address").is_not_null()
        & pl.col("address").str.to_uppercase().str.contains(search_addr.upper())
    ).sort("jurisdiction", "address").head(100)

    if len(matches) == 0:
        st.warning("No properties found matching that address.")
        st.caption(
            "Try a shorter search (e.g. street name only). "
            "Some jurisdictions may not have address data."
        )
        return

    st.write(f"**{len(matches)}** matching properties (showing up to 100)")

    st.caption("Click a row to view property details.")

    search_rows = []
    bbl_list = []
    for row in matches.iter_rows(named=True):
        bbl = row["bbl"]
        jur = row["jurisdiction"]
        addr = row["address"] or "(no address)"
        jur_display = _display_jurisdiction(jur)

        prop_viols = viols_df.filter(pl.col("bbl") == bbl)
        pv_stats = _property_violation_score(prop_viols)

        reg_id = row.get("registration_id")
        owner_match = owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()
        owner_label = "Owner on file" if len(owner_match) > 0 else "Not available"

        lk_level, lk_label, lk_color = _property_likert(pv_stats)

        search_rows.append({
            "Address": addr,
            "City": jur_display,
            "Rating": f"{lk_color} {lk_label}",
            "Violations": pv_stats["total"],
            "Critical": pv_stats["critical"],
            "Open": pv_stats["open"],
            "Owner": owner_label,
        })
        bbl_list.append(bbl)

    event = st.dataframe(
        search_rows,
        column_config={
            "Rating": st.column_config.TextColumn(
                "Rating",
                help="Property-level rating based on violation severity and status",
            ),
            "Violations": st.column_config.NumberColumn("Violations", format="%d"),
            "Critical": st.column_config.NumberColumn("Critical", format="%d"),
            "Open": st.column_config.NumberColumn("Open", format="%d"),
        },
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="addr_search_table",
        use_container_width=True,
    )

    if event.selection.rows:
        selected_bbl = bbl_list[event.selection.rows[0]]
        nav_link_property(selected_bbl)
        del st.session_state["addr_search_table"]
        st.rerun()


# =========================================================================
# PAGE: Property Detail
# =========================================================================
def page_property(bbl: str) -> None:
    prop_match = props_df.filter(pl.col("bbl") == bbl)

    if len(prop_match) == 0:
        st.error(f"Property not found: {bbl}")
        return

    prop = prop_match.row(0, named=True)
    jur = prop["jurisdiction"]
    addr = prop["address"] or bbl
    jur_display = _display_jurisdiction(jur)

    has_prior_search = "q" in st.query_params
    if has_prior_search:
        st.button("← Back to results", on_click=nav_link_back_to_results, key="bc_results")
    else:
        st.button("← Search another address", on_click=nav_link_address, key="bc_addr")
    st.title(f"🏠 {addr}")
    st.caption(DISCLAIMER)

    prop_viols = viols_df.filter(pl.col("bbl") == bbl).sort("inspection_date", descending=True)
    pv = _property_violation_score(prop_viols)

    lk_level, lk_label, lk_color = _property_likert(pv)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jurisdiction", jur_display)
    c2.metric("Rating", f"{lk_color} {lk_label}",
             help="Based on violation severity, count, and resolution status at this address")
    units = prop.get("units_residential")
    c3.metric("Residential Units", f"{units:,.0f}" if units else "Unknown")
    yb = prop.get("year_built")
    c4.metric("Year Built", yb or "Unknown")

    with st.expander("What does this rating mean?"):
        st.markdown(
            "The **rating** reflects the severity and resolution status of "
            "housing code violations at **this address**.\n\n"
            "| Rating | Meaning |\n"
            "|---|---|\n"
            "| 🟢 No issues found | Zero violations on record |\n"
            "| 🟢 Minor issues | Low-severity violations, all resolved |\n"
            "| 🟡 Some concerns | Open violations or moderate severity |\n"
            "| 🟠 Significant issues | Critical violations present |\n"
            "| 🔴 Severe issues | Critical violations + many unresolved |\n\n"
            f"**This property**: {pv['total']} violations, "
            f"{pv['critical']} critical, {pv['open']} open "
            f"({pv['open_pct']:.0%} unresolved)"
        )

    # --- Owner section (simplified — no owner_id, no confidence tier) ---
    st.divider()
    reg_id = prop.get("registration_id")
    owner_match = owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()

    if len(owner_match) > 0:
        owner_id = owner_match["owner_id"][0]
        score_match = df.filter(pl.col("owner_id") == owner_id) if len(df) > 0 else pl.DataFrame()

        if len(score_match) > 0:
            owner_row = score_match.row(0, named=True)
            o_lk_label = owner_row.get("likert_label", "Unknown")
            o_lk_color = owner_row.get("likert_color", "⚪")
            # Extract display name from owner_id ("john_smith [nyc]" → "John Smith")
            display_name = owner_id.split(" [")[0].replace("_", " ").title() if owner_id else "Unknown"
            st.subheader("Landlord Track Record")
            st.button(
                f"🔍 {display_name} — View full track record",
                on_click=nav_link_owner,
                args=(owner_id,),
                key="owner_link",
            )
            oc1, oc2, oc3 = st.columns(3)
            oc1.metric("Rating", f"{o_lk_color} {o_lk_label}",
                       help="Compares this landlord to all others in the same city")
            oc2.metric("Properties Managed", f"{owner_row['num_properties']:,}",
                       help="Distinct registered properties linked to this landlord")
            oc3.metric("Total Violations (all properties)", f"{owner_row['total_violations']:,}",
                       help="Sum of housing code violations across every property they manage")

            with st.expander("What does the landlord rating mean?"):
                st.markdown(
                    "The **landlord rating** is a composite score that compares "
                    "this owner's violation record against all other landlords "
                    "in the same jurisdiction.\n\n"
                    "It considers four factors:\n"
                    "- **Severity** — how serious the violations are\n"
                    "- **Density** — violations per residential unit\n"
                    "- **Spread** — how many of their properties have violations\n"
                    "- **Persistence** — what fraction remains unresolved\n\n"
                    "| Rating | Meaning |\n"
                    "|---|---|\n"
                    "| 🟢 Low concern | Better than ~80% of landlords |\n"
                    "| 🟡 Some concerns | Moderate violation history |\n"
                    "| 🟠 Moderate concerns | Above-average violation record |\n"
                    "| 🔴 Significant concerns | Worse than ~80% of landlords |\n"
                    "| 🔴 Severe concerns | Among the worst in the jurisdiction |\n\n"
                    f"**This landlord**: {owner_row['num_properties']:,} properties, "
                    f"{owner_row['total_violations']:,} total violations across all of them."
                )
        else:
            st.info("Owner identified but no harm score computed.")
    else:
        st.markdown(_no_owner_message(jur))

    # --- Violations ---
    st.divider()
    st.subheader("Violation History")

    if len(prop_viols) == 0:
        st.info("No violations recorded for this property in the analysis window.")
        return

    vc1, vc2, vc3 = st.columns(3)
    vc1.metric("Total Violations", f"{pv['total']:,}",
              help="All housing code violations recorded at this address since Jan 2022")
    vc2.metric("Open", f"{pv['open']:,}",
              help="Violations not yet remediated or dismissed")
    vc3.metric("Critical", f"{pv['critical']:,}",
              help="Tier 1 (most severe) — structural, fire safety, lead, vermin")

    sev_counts = prop_viols.group_by("severity_tier").len().sort("severity_tier")
    if len(sev_counts) > 0:
        sev_display = sev_counts.with_columns(
            pl.col("severity_tier").replace_strict(
                SEVERITY_LABELS, default="Unknown"
            ).alias("Severity"),
        ).select("Severity", pl.col("len").alias("Count"))
        st.bar_chart(sev_display.to_pandas(), x="Severity", y="Count")

    display_viols = prop_viols.select(
        pl.col("inspection_date").alias("Date"),
        pl.col("severity_tier").replace_strict(
            SEVERITY_LABELS, default="Unknown"
        ).alias("Severity"),
        pl.col("status").str.to_titlecase().alias("Status"),
    )

    page_size = 25
    n_total = pv["total"]
    total_pages = max((n_total + page_size - 1) // page_size, 1)
    viol_page = st.number_input(
        "Page", min_value=1, max_value=total_pages, value=1, key="viol_page"
    )
    st.dataframe(
        display_viols.slice((viol_page - 1) * page_size, page_size).to_pandas(),
        width="stretch",
        hide_index=True,
    )

    # --- PDF download ---
    st.divider()
    _pdf_owner_name = None
    _pdf_owner_rating = None
    _pdf_owner_props = None
    _pdf_owner_viols = None
    if len(owner_match) > 0:
        _oid = owner_match["owner_id"][0]
        _smatch = df.filter(pl.col("owner_id") == _oid) if len(df) > 0 else pl.DataFrame()
        if len(_smatch) > 0:
            _orow = _smatch.row(0, named=True)
            _pdf_owner_name = _oid.split(" [")[0].replace("_", " ").title()
            _pdf_owner_rating = f"{_orow.get('likert_color', '')} {_orow.get('likert_label', '')}".strip()
            _pdf_owner_props = _orow["num_properties"]
            _pdf_owner_viols = _orow["total_violations"]

    viol_rows = [
        {
            "date": str(r["inspection_date"]) if r["inspection_date"] else "",
            "severity": SEVERITY_LABELS.get(r["severity_tier"], "Unknown"),
            "status": (r["status"] or "").title(),
            "violation_id": r.get("violation_id", ""),
        }
        for r in prop_viols.iter_rows(named=True)
    ]

    pdf_bytes = generate_property_report(
        address=addr,
        jurisdiction=jur_display,
        rating_label=f"{lk_color} {lk_label}",
        units=f"{units:,.0f}" if units else "Unknown",
        year_built=yb or "Unknown",
        total_violations=pv["total"],
        critical=pv["critical"],
        open_violations=pv["open"],
        open_pct=pv["open_pct"],
        owner_name=_pdf_owner_name,
        owner_rating=_pdf_owner_rating,
        owner_properties=_pdf_owner_props,
        owner_total_violations=_pdf_owner_viols,
        violations=viol_rows,
    )
    safe_name = addr.replace(" ", "_").replace(",", "")[:50]
    st.download_button(
        "📄 Download Property Report (PDF)",
        data=pdf_bytes,
        file_name=f"renter_shield_{safe_name}.pdf",
        mime="application/pdf",
    )


# =========================================================================
# PAGE: Owner Detail (simplified for renters)
# =========================================================================
def page_owner_detail(owner_id: str) -> None:
    score_match = df.filter(pl.col("owner_id") == owner_id) if len(df) > 0 else df.clear()

    if len(score_match) == 0:
        st.error(f"Owner not found: {owner_id}")
        st.button("← Search another address", on_click=nav_link_address, key="bc_addr")
        return

    owner_row = score_match.row(0, named=True)
    jur = owner_row.get("jurisdiction", "")
    jur_display = _display_jurisdiction(jur)
    display_name = owner_id.split(" [")[0].replace("_", " ").title() if owner_id else "Unknown"
    o_lk_label = owner_row.get("likert_label", "Unknown")
    o_lk_color = owner_row.get("likert_color", "⚪")

    st.button("← Search another address", on_click=nav_link_address, key="bc_addr")
    st.title(f"🏠 Landlord: {display_name}")
    st.caption(DISCLAIMER)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jurisdiction", jur_display)
    c2.metric("Rating", f"{o_lk_color} {o_lk_label}",
             help="Compares this landlord to all others in the same city")
    c3.metric("Properties Managed", f"{owner_row['num_properties']:,}",
             help="Distinct registered properties linked to this landlord")
    c4.metric("Total Violations (all properties)", f"{owner_row['total_violations']:,}",
             help="Sum of housing code violations across every property they manage")

    with st.expander("What does the landlord rating mean?"):
        st.markdown(
            "The **landlord rating** is a composite score that compares "
            "this owner's violation record against all other landlords "
            "in the same jurisdiction.\n\n"
            "It considers four factors:\n"
            "- **Severity** — how serious the violations are\n"
            "- **Density** — violations per residential unit\n"
            "- **Spread** — how many of their properties have violations\n"
            "- **Persistence** — what fraction remains unresolved\n\n"
            "| Rating | Meaning |\n"
            "|---|---|\n"
            "| 🟢 Low concern | Better than ~80% of landlords |\n"
            "| 🟡 Some concerns | Moderate violation history |\n"
            "| 🟠 Moderate concerns | Above-average violation record |\n"
            "| 🔴 Significant concerns | Worse than ~80% of landlords |\n"
            "| 🔴 Severe concerns | Among the worst in the jurisdiction |"
        )

    # --- Properties owned ---
    st.divider()
    st.subheader("Properties")

    owner_regs = owner_reg_df.filter(pl.col("owner_id") == owner_id)
    if len(owner_regs) > 0:
        reg_ids = owner_regs["registration_id"].to_list()
        owner_props = props_df.filter(pl.col("registration_id").is_in(reg_ids))
    else:
        owner_props = props_df.clear()

    if len(owner_props) == 0:
        st.info("No linked properties found for this owner.")
    else:
        st.caption("Click a row to view property details.")

        prop_rows = []
        prop_bbls = []
        for row in owner_props.sort("address").iter_rows(named=True):
            bbl = row["bbl"]
            addr = row["address"] or bbl
            prop_viols = viols_df.filter(pl.col("bbl") == bbl)
            pv_stats = _property_violation_score(prop_viols)
            lk_level, lk_label, lk_color = _property_likert(pv_stats)

            prop_rows.append({
                "Address": addr,
                "Rating": f"{lk_color} {lk_label}",
                "Violations": pv_stats["total"],
                "Critical": pv_stats["critical"],
                "Open": pv_stats["open"],
            })
            prop_bbls.append(bbl)

        event = st.dataframe(
            prop_rows,
            column_config={
                "Rating": st.column_config.TextColumn(
                    "Rating",
                    help="Property-level rating based on violation severity and status",
                ),
                "Violations": st.column_config.NumberColumn("Violations", format="%d"),
                "Critical": st.column_config.NumberColumn("Critical", format="%d"),
                "Open": st.column_config.NumberColumn("Open", format="%d"),
            },
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="owner_props_table",
            use_container_width=True,
        )

        if event.selection.rows:
            selected_bbl = prop_bbls[event.selection.rows[0]]
            nav_link_property(selected_bbl)
            del st.session_state["owner_props_table"]
            st.rerun()


# =========================================================================
# Router
# =========================================================================
if page == "property" and nav_bbl:
    log_page_view(_audit_user["id"], "renter", "property", {"bbl": nav_bbl})
    page_property(nav_bbl)
elif page == "owner" and nav_owner:
    log_page_view(_audit_user["id"], "renter", "owner", {"owner": nav_owner})
    page_owner_detail(nav_owner)
else:
    log_page_view(_audit_user["id"], "renter", "address_search")
    page_address_search()
