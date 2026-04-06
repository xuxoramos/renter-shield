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
    "hud_reac": "HUD REAC (Federal)",
    "sf": "San Francisco",
    "seattle": "Seattle",
    "pittsburgh": "Pittsburgh",
}

# Jurisdictions where we have owner/contact data for scoring
_OWNER_DATA_JURISDICTIONS = {"nyc", "boston", "philadelphia", "chicago", "pittsburgh", "hud_reac"}

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
    if jurisdiction not in _OWNER_DATA_JURISDICTIONS:
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

    for row in matches.iter_rows(named=True):
        bbl = row["bbl"]
        jur = row["jurisdiction"]
        addr = row["address"] or "(no address)"
        jur_display = JURISDICTION_DISPLAY.get(jur, jur.title())

        prop_viols = viols_df.filter(pl.col("bbl") == bbl)
        n_viols = len(prop_viols)
        n_critical = len(prop_viols.filter(pl.col("severity_tier") == 1))
        n_open = len(prop_viols.filter(pl.col("status") == "open"))
        pv_stats = _property_violation_score(prop_viols)

        # Simplified owner signal — no owner_id exposed
        reg_id = row.get("registration_id")
        owner_match = owner_reg_df.filter(pl.col("registration_id") == reg_id) if reg_id else pl.DataFrame()
        owner_id = owner_match["owner_id"][0] if len(owner_match) > 0 else None
        owner_label = "Owner on file" if owner_id else "Owner: not available"

        # Likert rating for this property
        lk_level, lk_label, lk_color = _property_likert(pv_stats)

        cols = st.columns([3, 2, 1, 2, 2, 1])
        cols[0].write(f"**{addr}**")
        cols[1].write(f"📍 {jur_display}")
        cols[2].write(f"{lk_color} **{lk_label}**")
        viol_text = f"{n_viols:,} violations"
        if n_critical > 0:
            viol_text += f" · 🔴 {n_critical} critical"
        if n_open > 0:
            viol_text += f" · {n_open} open"
        cols[3].write(viol_text)
        cols[4].caption(owner_label)
        cols[5].button(
            "Details",
            key=f"prop_{bbl}",
            on_click=nav_link_property,
            args=(bbl,),
        )


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
    jur_display = JURISDICTION_DISPLAY.get(jur, jur.title())

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
    c2.metric("Rating", f"{lk_color} {lk_label}")
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
            oc1.metric("Rating", f"{o_lk_color} {o_lk_label}")
            oc2.metric("Properties Managed", f"{owner_row['num_properties']:,}")
            oc3.metric("Total Violations (all properties)", f"{owner_row['total_violations']:,}")
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
    vc1.metric("Total Violations", f"{pv['total']:,}")
    vc2.metric("Open", f"{pv['open']:,}")
    vc3.metric("Critical", f"{pv['critical']:,}")

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
    jur_display = JURISDICTION_DISPLAY.get(jur, jur.title())
    display_name = owner_id.split(" [")[0].replace("_", " ").title() if owner_id else "Unknown"
    o_lk_label = owner_row.get("likert_label", "Unknown")
    o_lk_color = owner_row.get("likert_color", "⚪")

    st.button("← Search another address", on_click=nav_link_address, key="bc_addr")
    st.title(f"🏠 Landlord: {display_name}")
    st.caption(DISCLAIMER)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jurisdiction", jur_display)
    c2.metric("Rating", f"{o_lk_color} {o_lk_label}")
    c3.metric("Properties Managed", f"{owner_row['num_properties']:,}")
    c4.metric("Total Violations (all properties)", f"{owner_row['total_violations']:,}")

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
        for idx, row in enumerate(owner_props.sort("address").iter_rows(named=True)):
            bbl = row["bbl"]
            addr = row["address"] or bbl
            prop_viols = viols_df.filter(pl.col("bbl") == bbl)
            n_viols = len(prop_viols)
            n_critical = len(prop_viols.filter(pl.col("severity_tier") == 1))
            n_open = len(prop_viols.filter(pl.col("status") == "open"))
            pv_stats = _property_violation_score(prop_viols)
            lk_level, lk_label, lk_color = _property_likert(pv_stats)

            cols = st.columns([3, 1, 2, 1])
            cols[0].write(f"**{addr}**")
            cols[1].write(f"{lk_color} **{lk_label}**")
            viol_text = f"{n_viols:,} violations"
            if n_critical > 0:
                viol_text += f" · 🔴 {n_critical} critical"
            if n_open > 0:
                viol_text += f" · {n_open} open"
            cols[2].write(viol_text)
            cols[3].button(
                "Details",
                key=f"owner_prop_{bbl}_{idx}",
                on_click=nav_link_property,
                args=(bbl,),
            )


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
