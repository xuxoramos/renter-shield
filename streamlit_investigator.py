"""Streamlit dashboard — Investigator-facing harm score explorer.

Pages:
  1. Overview (default) — jurisdiction cards with summary stats
  2. Jurisdiction — ranked owner table for a single city
  3. Owner Detail — full score breakdown for one landlord

Run with:
    streamlit run streamlit_investigator.py

Expects ``output/all_landlords_harm_scores.parquet`` (produced by the pipeline).
"""

import math
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

DISCLAIMER = (
    "**Disclaimer:** Scores are derived from publicly available government "
    "records and algorithmic analysis. They do not constitute legal findings, "
    "may not reflect current conditions, and may contain errors due to "
    "name-based ownership resolution. Independent verification is required "
    "before any legal, administrative, or public action."
)

CONFIDENCE_LABELS = {
    "high": "🟢 High",
    "medium": "🟡 Medium",
    "low": "🟠 Low",
}

CONFIDENCE_DESCRIPTIONS = {
    "high": "Name + address corroborated — safe for filings",
    "medium": "Name match only — needs corroboration",
    "low": "Address grouping only — investigative lead",
}

JURISDICTION_DISPLAY = {
    "nyc": "New York City",
    "boston": "Boston",
    "philadelphia": "Philadelphia",
    "chicago": "Chicago",
    "hud_reac": "HUD REAC (Federal)",
    "sf": "San Francisco",
    "seattle": "Seattle",
    "pittsburgh": "Pittsburgh",
    "la": "Los Angeles",
    "austin": "Austin",
    "miami": "Miami-Dade",
    "detroit": "Detroit",
}


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_data
def load_scores() -> pl.DataFrame:
    if not SCORES_FILE.exists():
        st.error(f"Scores file not found: {SCORES_FILE}. Run the pipeline first.")
        st.stop()
    return pl.read_parquet(SCORES_FILE).with_columns(
        pl.col("severity_score").cast(pl.Float64),
    )


PROPERTIES_FILE = OUTPUT_DIR / "properties.parquet"
VIOLATIONS_FILE = OUTPUT_DIR / "violations.parquet"


@st.cache_data
def load_properties() -> pl.DataFrame:
    if not PROPERTIES_FILE.exists():
        return pl.DataFrame()
    return pl.read_parquet(PROPERTIES_FILE)


@st.cache_data
def load_violations() -> pl.DataFrame:
    if not VIOLATIONS_FILE.exists():
        return pl.DataFrame()
    return pl.read_parquet(VIOLATIONS_FILE)


# ---------------------------------------------------------------------------
# Page config + data
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Landlord Harm Score Explorer — Investigator",
    page_icon="🏚️",
    layout="wide",
)

df = load_scores()
props_df = load_properties()
viols_df = load_violations()

# ---------------------------------------------------------------------------
# Registration gate — must register before seeing any data
# ---------------------------------------------------------------------------
_audit_user = require_registration("investigator")
if _audit_user is None:
    st.stop()

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
page = st.query_params.get("page", "overview")
nav_jur = st.query_params.get("jur", None)
nav_owner = st.query_params.get("owner", None)


def nav_link_jur(jur: str) -> None:
    st.query_params["page"] = "jurisdiction"
    st.query_params["jur"] = jur
    if "owner" in st.query_params:
        del st.query_params["owner"]


def nav_link_owner(owner_id: str) -> None:
    st.query_params["page"] = "owner"
    st.query_params["owner"] = owner_id


def nav_link_overview() -> None:
    st.query_params["page"] = "overview"
    for k in ["jur", "owner"]:
        if k in st.query_params:
            del st.query_params[k]


# =========================================================================
# PAGE: Overview
# =========================================================================
def page_overview() -> None:
    st.title("🏚️ Landlord Harm Score Explorer")
    st.caption(DISCLAIMER)

    st.markdown("Select a jurisdiction to explore scored landlords.")

    # --- scored jurisdiction stats (from owner scores) ---
    jur_stats = (
        df.group_by("jurisdiction")
        .agg(
            pl.len().alias("owners"),
            pl.col("total_violations").sum().alias("violations"),
            pl.col("class_c_violations").sum().alias("critical"),
            pl.col("total_harm_score").mean().alias("avg_score"),
            pl.col("total_harm_score").max().alias("max_score"),
            pl.col("num_properties").sum().alias("properties"),
        )
        .sort("owners", descending=True)
    )
    scored_jurs = set(jur_stats["jurisdiction"].to_list())

    # --- unscored jurisdiction stats (from raw data) ---
    all_jurs = set()
    if len(viols_df) > 0:
        all_jurs |= set(viols_df["jurisdiction"].unique().to_list())
    if len(props_df) > 0:
        all_jurs |= set(props_df["jurisdiction"].unique().to_list())
    unscored_jurs = sorted(all_jurs - scored_jurs)

    unscored_stats = []
    for jur in unscored_jurs:
        jv = viols_df.filter(pl.col("jurisdiction") == jur) if len(viols_df) > 0 else pl.DataFrame()
        jp = props_df.filter(pl.col("jurisdiction") == jur) if len(props_df) > 0 else pl.DataFrame()
        n_viols = len(jv)
        n_crit = len(jv.filter(pl.col("severity_tier") == 1)) if n_viols else 0
        n_props = len(jp)
        unscored_stats.append({
            "jurisdiction": jur,
            "violations": n_viols,
            "critical": n_crit,
            "properties": n_props,
        })

    conf_counts = df.group_by("confidence").len().sort("confidence")

    total_viols = df["total_violations"].sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jurisdictions", len(scored_jurs) + len(unscored_jurs))
    c2.metric("Scored owners", f"{len(df):,}")
    c3.metric("Total violations", f"{total_viols:,}")
    for row in conf_counts.iter_rows(named=True):
        label = CONFIDENCE_LABELS.get(row["confidence"], row["confidence"])
        c4.metric(label, f"{row['len']:,}")

    st.divider()

    # --- scored jurisdiction cards ---
    card_rows = list(jur_stats.iter_rows(named=True))

    for i in range(0, len(card_rows), 3):
        cols = st.columns(3)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(card_rows):
                break
            row = card_rows[idx]
            jur = row["jurisdiction"]
            display = JURISDICTION_DISPLAY.get(jur, jur.title())

            with col:
                st.subheader(display)
                m1, m2 = st.columns(2)
                m1.metric("Owners", f"{row['owners']:,}")
                m2.metric("Avg Score", f"{row['avg_score']:,.0f}")
                m3, m4 = st.columns(2)
                m3.metric("Violations", f"{row['violations']:,}")
                m4.metric("Critical", f"{row['critical']:,}")
                st.button(
                    f"Explore {display} →",
                    key=f"btn_{jur}",
                    on_click=nav_link_jur,
                    args=(jur,),
                    use_container_width=True,
                )

    # --- unscored jurisdiction cards ---
    if unscored_stats:
        st.divider()
        st.subheader("Jurisdictions Without Owner Data")
        st.caption(
            "These cities have violation and property data but no publicly "
            "available owner/contact records. Address searches and property "
            "lookups work, but no landlord harm scores are available."
        )

        for i in range(0, len(unscored_stats), 3):
            cols = st.columns(3)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(unscored_stats):
                    break
                us = unscored_stats[idx]
                jur = us["jurisdiction"]
                display = JURISDICTION_DISPLAY.get(jur, jur.title())

                with col:
                    st.subheader(f"{display}  ⚠️")
                    m1, m2 = st.columns(2)
                    m1.metric("Violations", f"{us['violations']:,}")
                    m2.metric("Critical", f"{us['critical']:,}")
                    st.metric("Properties", f"{us['properties']:,}")
                    st.button(
                        f"Browse {display} →",
                        key=f"btn_{jur}",
                        on_click=nav_link_jur,
                        args=(jur,),
                        use_container_width=True,
                    )

    # Cross-jurisdiction search
    st.divider()
    st.subheader("🔍 Search across all jurisdictions")
    search_q = st.text_input(
        "Owner name search",
        placeholder="e.g. WALNUT CAPITAL, SMITH, ARVERNE",
        key="global_search",
    )
    if search_q:
        matches = df.filter(
            pl.col("owner_id").str.to_uppercase().str.contains(search_q.upper())
        ).sort("total_harm_score", descending=True).head(50)

        if len(matches) == 0:
            st.info("No matches found.")
        else:
            st.write(f"**{len(matches)}** results (showing top 50)")
            for row in matches.iter_rows(named=True):
                conf = CONFIDENCE_LABELS.get(row["confidence"], row["confidence"])
                jur_display = JURISDICTION_DISPLAY.get(row["jurisdiction"], row["jurisdiction"])
                cols = st.columns([4, 2, 2, 2, 1])
                cols[0].write(f"**{row['owner_id']}**")
                cols[1].write(jur_display)
                cols[2].write(f"Score: {row['total_harm_score']:,.0f}")
                cols[3].write(conf)
                cols[4].button(
                    "→",
                    key=f"search_{row['owner_id']}",
                    on_click=nav_link_owner,
                    args=(row["owner_id"],),
                )

    with st.expander("📖 Methodology"):
        _render_methodology()


# =========================================================================
# PAGE: Jurisdiction
# =========================================================================
def page_jurisdiction(jur: str) -> None:
    display = JURISDICTION_DISPLAY.get(jur, jur.title())
    jur_df = df.filter(pl.col("jurisdiction") == jur)

    # Check if this is an unscored jurisdiction
    is_unscored = len(jur_df) == 0
    if is_unscored:
        # Show violation/property stats from raw data
        jur_viols = viols_df.filter(pl.col("jurisdiction") == jur) if len(viols_df) > 0 else pl.DataFrame()
        jur_props = props_df.filter(pl.col("jurisdiction") == jur) if len(props_df) > 0 else pl.DataFrame()

        if len(jur_viols) == 0 and len(jur_props) == 0:
            st.error(f"No data for jurisdiction: {jur}")
            return

        st.button("← All jurisdictions", on_click=nav_link_overview)
        st.title(f"🏚️ {display}  ⚠️")
        st.caption(DISCLAIMER)
        st.warning(
            "**No owner data available.** This jurisdiction has violation and "
            "property records but no publicly available owner/contact data. "
            "Landlord harm scores cannot be computed. Address searches and "
            "property-level violation counts are available below."
        )

        n_viols = len(jur_viols)
        n_crit = len(jur_viols.filter(pl.col("severity_tier") == 1)) if n_viols else 0
        n_open = len(jur_viols.filter(pl.col("status") == "open")) if n_viols else 0
        n_props = len(jur_props)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Properties", f"{n_props:,}")
        c2.metric("Violations", f"{n_viols:,}")
        c3.metric("Critical", f"{n_crit:,}")
        c4.metric("Open", f"{n_open:,}")

        # Severity distribution
        if n_viols > 0:
            st.divider()
            st.subheader("Violation Severity Distribution")
            sev_dist = (
                jur_viols.group_by("severity_tier")
                .len()
                .sort("severity_tier")
            )
            tier_labels = {1: "Critical", 2: "Serious", 3: "Minor", 4: "Info"}
            chart_data = pl.DataFrame({
                "Tier": [tier_labels.get(r["severity_tier"], str(r["severity_tier"])) for r in sev_dist.iter_rows(named=True)],
                "Count": [r["len"] for r in sev_dist.iter_rows(named=True)],
            })
            st.bar_chart(chart_data.to_pandas(), x="Tier", y="Count")

        # Address search within this jurisdiction
        st.divider()
        st.subheader("🔍 Search addresses in this jurisdiction")
        addr_q = st.text_input(
            "Address",
            placeholder="e.g. MAIN ST, 1234 BROADWAY",
            key="unscored_addr_search",
        )
        if addr_q and len(addr_q) >= 3 and len(jur_props) > 0:
            addr_matches = jur_props.filter(
                pl.col("address").is_not_null()
                & pl.col("address").str.to_uppercase().str.contains(addr_q.upper())
            ).sort("address").head(100)

            if len(addr_matches) == 0:
                st.info("No matching addresses.")
            else:
                st.write(f"**{len(addr_matches)}** results (up to 100)")
                for row in addr_matches.iter_rows(named=True):
                    bbl = row["bbl"]
                    addr = row["address"] or "(no address)"
                    pv = jur_viols.filter(pl.col("bbl") == bbl) if n_viols else pl.DataFrame()
                    nv = len(pv)
                    nc = len(pv.filter(pl.col("severity_tier") == 1)) if nv else 0
                    cols = st.columns([4, 2, 2])
                    cols[0].write(f"**{addr}**")
                    cols[1].write(f"{nv:,} violations")
                    cols[2].write(f"{nc:,} critical" if nc else "—")
        return

    # --- scored jurisdiction (original logic) ---

    st.button("← All jurisdictions", on_click=nav_link_overview)
    st.title(f"🏚️ {display}")
    st.caption(DISCLAIMER)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Scored owners", f"{len(jur_df):,}")
    c2.metric("Violations", f"{jur_df['total_violations'].sum():,}")
    c3.metric("Critical", f"{jur_df['class_c_violations'].sum():,}")
    c4.metric("Avg score", f"{jur_df['total_harm_score'].mean():,.0f}")
    c5.metric("Max score", f"{jur_df['total_harm_score'].max():,.0f}")

    conf_counts = jur_df.group_by("confidence").len().sort("confidence")
    if len(conf_counts) > 0:
        conf_cols = st.columns(len(conf_counts))
        for i, row in enumerate(conf_counts.iter_rows(named=True)):
            label = CONFIDENCE_LABELS.get(row["confidence"], row["confidence"])
            desc = CONFIDENCE_DESCRIPTIONS.get(row["confidence"], "")
            conf_cols[i].metric(label, f"{row['len']:,}")
            conf_cols[i].caption(desc)

    st.divider()

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        min_score = st.number_input("Min harm score", min_value=0.0, value=0.0, step=100.0, key="jur_min")
    with fc2:
        min_props = st.slider("Min properties", 1, 50, 3, key="jur_props")
    with fc3:
        name_search = st.text_input("Name search", placeholder="e.g. SMITH", key="jur_name")

    filtered = jur_df.filter(
        (pl.col("total_harm_score") >= min_score)
        & (pl.col("num_properties") >= min_props)
    )
    if name_search:
        filtered = filtered.filter(
            pl.col("owner_id").str.to_uppercase().str.contains(name_search.upper())
        )

    st.write(f"**{len(filtered):,}** owners match filters")

    if len(filtered) > 0:
        scores = filtered["total_harm_score"].to_list()
        max_log = max(math.ceil(math.log10(max(scores) + 1)), 1)
        bins = [0] + [10**i for i in range(1, max_log + 1)]
        labels = [f"<{10**i:,}" for i in range(1, max_log + 1)]
        counts = []
        for lo, hi in zip(bins[:-1], bins[1:]):
            n = len(filtered.filter(
                (pl.col("total_harm_score") >= lo) & (pl.col("total_harm_score") < hi)
            ))
            counts.append(n)
        chart_df = pl.DataFrame({"Score range": labels, "Owners": counts})
        st.bar_chart(chart_df.to_pandas(), x="Score range", y="Owners")

    st.subheader("Ranked Owners")
    page_size = 25
    total_pages = max((len(filtered) + page_size - 1) // page_size, 1)
    tbl_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, key="tbl_page")
    start = (tbl_page - 1) * page_size

    page_df = filtered.slice(start, page_size)

    for row in page_df.iter_rows(named=True):
        conf_badge = CONFIDENCE_LABELS.get(row["confidence"], row["confidence"])
        lk_color = row.get("likert_color", "")
        lk_label = row.get("likert_label", "")
        cols = st.columns([4, 1, 1, 2, 2, 2, 1])
        cols[0].write(f"**{row['owner_id']}**")
        cols[1].write(conf_badge)
        cols[2].write(f"{lk_color} {lk_label}")
        cols[3].write(f"Score: **{row['total_harm_score']:,.0f}**")
        cols[4].write(f"{row['num_properties']} props · {row['total_violations']:,} violations")
        cols[5].write(f"{row['class_c_violations']:,} critical · {row['unresolved_violations']:,} open")
        cols[6].button(
            "Details",
            key=f"detail_{row['owner_id']}",
            on_click=nav_link_owner,
            args=(row["owner_id"],),
        )


# =========================================================================
# PAGE: Owner Detail
# =========================================================================
def page_owner(owner_id: str) -> None:
    match = df.filter(pl.col("owner_id") == owner_id)

    if len(match) == 0:
        st.error(f"Owner not found: {owner_id}")
        return

    row = match.row(0, named=True)
    jur = row["jurisdiction"]
    display_jur = JURISDICTION_DISPLAY.get(jur, jur.title())
    conf = row["confidence"]

    bc1, bc2 = st.columns([1, 1])
    with bc1:
        st.button("← All jurisdictions", on_click=nav_link_overview, key="bc_overview")
    with bc2:
        st.button(f"← {display_jur}", on_click=nav_link_jur, args=(jur,), key="bc_jur")

    st.title(f"🏚️ {row['owner_id']}")
    st.caption(DISCLAIMER)

    conf_label = CONFIDENCE_LABELS.get(conf, conf)
    conf_desc = CONFIDENCE_DESCRIPTIONS.get(conf, "")
    if conf == "high":
        st.success(f"**{conf_label}** — {conf_desc}")
    elif conf == "medium":
        st.warning(f"**{conf_label}** — {conf_desc}")
    else:
        st.info(f"**{conf_label}** — {conf_desc}")

    st.divider()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jurisdiction", display_jur)
    c2.metric("Legacy Harm Score", f"{row['total_harm_score']:,.0f}")
    svi = row.get("svi_composite")
    c3.metric("SVI Composite", f"{svi:.2f}" if svi is not None else "N/A")
    lk_color = row.get("likert_color", "")
    lk_label = row.get("likert_label", "")
    c4.metric("Rating", f"{lk_color} {lk_label}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Properties", f"{row['num_properties']:,}")
    c2.metric("Total Violations", f"{row['total_violations']:,}")
    c3.metric("Critical (Tier 1)", f"{row['class_c_violations']:,}")
    c4.metric("Unresolved", f"{row['unresolved_violations']:,}")

    c1, c2, _c3, _c4 = st.columns(4)
    c1.metric("Total Units", f"{row['total_units']:,.0f}")

    st.divider()
    st.subheader("Score Breakdown")

    # SVI theme percentiles
    st.markdown("**SVI Theme Percentiles** (within-jurisdiction or pooled for small jurisdictions)")
    theme_sev = row.get("theme_severity")
    theme_port = row.get("theme_portfolio")
    theme_comp = row.get("theme_compliance")
    tc1, tc2, tc3, tc4 = st.columns(4)
    tc1.metric("Severity Theme", f"{theme_sev:.2f}" if theme_sev is not None else "N/A")
    tc2.metric("Portfolio Theme", f"{theme_port:.2f}" if theme_port is not None else "N/A")
    tc3.metric("Compliance Theme", f"{theme_comp:.2f}" if theme_comp is not None else "N/A")
    tc4.metric("SVI Composite", f"{svi:.2f}" if svi is not None else "N/A")

    st.markdown("**Legacy Weighted-Linear Breakdown**")

    breakdown = {
        "Severity (40%)": float(row["severity_score"]) * 0.4,
        "Density (30%)": float(row["density_score"]) * 100 * 0.3,
        "Widespread (20%)": float(row["widespread_score"]) * 100 * 0.2,
        "Persistence (10%)": float(row["persistence_score"]) * 100 * 0.1,
    }
    breakdown_df = pl.DataFrame({
        "Component": list(breakdown.keys()),
        "Weighted Score": list(breakdown.values()),
    })
    st.bar_chart(breakdown_df.to_pandas(), x="Component", y="Weighted Score")

    st.markdown("""
| Component | Raw Value | Weight | Weighted |
|---|---|---|---|
| Severity | {sev:,.1f} | 40% | {sev_w:,.1f} |
| Density (viols/unit) | {dens:.2f} | 30% | {dens_w:,.1f} |
| Widespread (%) | {wide:.1%} | 20% | {wide_w:,.1f} |
| Persistence (%) | {pers:.1%} | 10% | {pers_w:,.1f} |
    """.format(
        sev=float(row["severity_score"]),
        sev_w=breakdown["Severity (40%)"],
        dens=float(row["density_score"]),
        dens_w=breakdown["Density (30%)"],
        wide=float(row["widespread_score"]),
        wide_w=breakdown["Widespread (20%)"],
        pers=float(row["persistence_score"]),
        pers_w=breakdown["Persistence (10%)"],
    ))

    with st.expander("Raw metrics (JSON)"):
        st.json({
            k: (float(v) if isinstance(v, (int, float)) else str(v))
            for k, v in row.items()
        })


# =========================================================================
# Methodology
# =========================================================================
def _render_methodology() -> None:
    st.markdown("""
### Harm Score Formula

```
Harm Score = (Severity × 0.4) + (Density × 100 × 0.3)
           + (Widespread × 100 × 0.2) + (Persistence × 100 × 0.1)
```

| Component | Weight | Description |
|-----------|--------|-------------|
| **Severity** | 40% | Weighted sum: Critical = 5 pts, Serious = 2.5, Minor = 1, Info = 0 |
| **Density** | 30% | Violations ÷ residential units |
| **Widespread** | 20% | Properties with violations ÷ total properties |
| **Persistence** | 10% | Open violations ÷ total violations |

### Confidence Levels

| Level | Meaning | Recommended Use |
|-------|---------|-----------------|
| 🟢 **High** | Name matches AND shares a business address | Safe for filings and subpoenas |
| 🟡 **Medium** | Name matches, ≤3 addresses, no overlap | Investigate further before formal action |
| 🟠 **Low** | Same business address, different names | Investigative lead only |

Names appearing at >3 distinct addresses without corroboration are excluded
as likely name collisions.

### Data Sources

All data is from publicly available government open-data portals.
Violation records cover January 2022 through the most recent download.
    """)


# =========================================================================
# Router
# =========================================================================
if page == "jurisdiction" and nav_jur:
    log_page_view(_audit_user["id"], "investigator", "jurisdiction", {"jur": nav_jur})
    page_jurisdiction(nav_jur)
elif page == "owner" and nav_owner:
    log_page_view(_audit_user["id"], "investigator", "owner", {"owner": nav_owner})
    page_owner(nav_owner)
else:
    log_page_view(_audit_user["id"], "investigator", "overview")
    page_overview()
