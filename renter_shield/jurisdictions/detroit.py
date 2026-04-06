"""Detroit jurisdiction adapter — Blight Tickets (property maintenance violations).

Data source (ArcGIS Feature Service, data.detroitmi.gov):
  - Blight Tickets: ~876K blight violation tickets issued by city inspectors,
    police, and other officials for Detroit City Code property maintenance
    violations (Article 15).
    Fields: ticket_id, ticket_number, address, ordinance_law,
    ordinance_description, disposition, agency_name, ticket_issued_date,
    amt_fine, amt_judgment, parcel_id, property_owner_id,
    property_owner_name, property_owner_address, property_owner_city,
    property_owner_state, property_owner_zip_code, zip_code,
    street_number, street_prefix, street_name, street_type, neighborhood.

Ordinance description severity mapping rationale:
  - Tier 1 (Critical): lead clearance violations (health hazard),
    unsafe/hazardous structure
  - Tier 2 (Serious): rodent harborage, failure to maintain structure/
    accessory structure, failure to obtain compliance/registration
    certificates, graffiti, snow/ice
  - Tier 3 (Minor): weeds/overgrowth, trash/debris, parking on lawn,
    container placement, bulk solid waste

Owner data:
  - property_owner_name is available for most tickets — this gives us
    owner-level scoring capability.
  - property_owner_address/city/state/zip also available.
  - parcel_id from City Assessor.
"""

from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# ArcGIS REST endpoint
_BLIGHT_URL = (
    "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services"
    "/blight_tickets/FeatureServer/0"
)

# Pagination settings (maxRecordCount = 1000 for this service)
_PAGE_SIZE = 1000
_TIMEOUT = 120
_RETRIES = 3

# ---- Severity mapping by ordinance_description keywords ----
# Matched case-insensitively against ordinance_description
_CRITICAL_KEYWORDS = [
    "lead clearance",          # lead paint hazard
    "lead-based paint",        # lead paint
    "unsafe",                  # unsafe structure/conditions
    "hazardous",               # hazardous waste/conditions
]
_SERIOUS_KEYWORDS = [
    "rodent",                  # rodent harborage / infestation
    "failure to maintain",     # structural maintenance
    "certificate of compliance",  # missing compliance cert
    "certificate of registration",  # missing rental registration
    "graffiti",                # blight / graffiti
    "snow and ice",            # sidewalk safety
    "removal of snow",         # sidewalk safety
    "accessory structure",     # outbuilding maintenance
    "vacant building",         # vacant/abandoned property
]
# Everything else defaults to Tier 3 (weeds, trash, parking, containers)


def _arcgis_paginated_get(
    base_url: str,
    *,
    where: str = "1=1",
    out_fields: str = "*",
    page_size: int = _PAGE_SIZE,
) -> list[dict]:
    """Fetch all features from an ArcGIS FeatureServer using offset pagination."""
    all_features: list[dict] = []
    offset = 0
    while True:
        params = (
            f"where={urllib.request.quote(where)}"
            f"&outFields={urllib.request.quote(out_fields)}"
            f"&returnGeometry=false"
            f"&f=json"
            f"&resultOffset={offset}"
            f"&resultRecordCount={page_size}"
        )
        url = f"{base_url}/query?{params}"
        for attempt in range(1, _RETRIES + 1):
            try:
                req = urllib.request.Request(url)
                req.add_header("User-Agent", "renter-shield/1.0")
                with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                    data = json.loads(resp.read().decode())
                break
            except Exception:
                if attempt == _RETRIES:
                    raise
                wait = 2 ** attempt
                print(f"  retry {attempt}/{_RETRIES} in {wait}s…")
                time.sleep(wait)
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(f["attributes"] for f in features)
        if len(all_features) % 10_000 < page_size:
            print(f"  fetched {len(all_features)} features…")
        if not data.get("exceededTransferLimit", False) and len(features) < page_size:
            break
        offset += len(features)
    return all_features


class DetroitAdapter(JurisdictionAdapter):
    jurisdiction_code = "detroit"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # ticket_issued_date is esriFieldTypeDateOnly — use DATE literal
        # Also cap at 2026-12-31 to exclude data-entry errors (years 3016, etc.)
        where = f"ticket_issued_date >= DATE '{MIN_DATE}' AND ticket_issued_date <= DATE '2026-12-31'"
        print("[detroit] downloading blight tickets (ArcGIS paginated)…")
        rows = _arcgis_paginated_get(_BLIGHT_URL, where=where)
        print(f"[detroit] total rows: {len(rows)}")

        if not rows:
            print("[detroit] WARNING: no rows returned")
            return

        # Normalize all values to strings — ArcGIS returns mixed types
        # (e.g. zip_code as int for US, string for Canadian addresses)
        for row in rows:
            for k, v in row.items():
                if v is not None and not isinstance(v, str):
                    row[k] = str(v)

        df = pl.DataFrame(rows, infer_schema_length=None)
        out = self.data_dir / "detroit_blight.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[detroit] saved {len(df)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "detroit_blight.parquet")

        raw = raw.filter(
            pl.col("ticket_issued_date").is_not_null()
            & (pl.col("ticket_issued_date").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int32) <= 2026)
        )

        # BBL: prefer parcel_id if available, fall back to address-derived
        bbl_expr = (
            pl.when(pl.col("parcel_id").is_not_null() & (pl.col("parcel_id").cast(pl.Utf8).str.strip_chars() != ""))
            .then(pl.lit("det-") + pl.col("parcel_id").cast(pl.Utf8).str.strip_chars())
            .otherwise(
                pl.lit("det-addr-")
                + pl.col("address").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"[^A-Z0-9]", "")
            )
        )

        # Severity mapping by ordinance_description keyword matching
        desc_lower = pl.col("ordinance_description").cast(pl.Utf8).str.to_lowercase()
        severity_expr = (
            pl.when(_matches_any(desc_lower, _CRITICAL_KEYWORDS))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(_matches_any(desc_lower, _SERIOUS_KEYWORDS))
            .then(pl.lit(2, dtype=pl.Int8))
            .otherwise(pl.lit(3, dtype=pl.Int8))
        )

        # Disposition → status mapping
        disp_lower = pl.col("disposition").cast(pl.Utf8).str.to_lowercase()
        status_expr = (
            pl.when(
                disp_lower.str.contains("responsible")
                | disp_lower.str.contains("not responsible")
                | disp_lower.str.contains("paid")
            )
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        # ticket_issued_date is DateOnly — comes as "YYYY-MM-DD" string
        date_expr = pl.col("ticket_issued_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d")

        return raw.select(
            pl.col("ticket_number").cast(pl.Utf8).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            date_expr.alias("inspection_date"),
            pl.lit("detroit").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "detroit_blight.parquet")

        bbl_expr = (
            pl.when(pl.col("parcel_id").is_not_null() & (pl.col("parcel_id").cast(pl.Utf8).str.strip_chars() != ""))
            .then(pl.lit("det-") + pl.col("parcel_id").cast(pl.Utf8).str.strip_chars())
            .otherwise(
                pl.lit("det-addr-")
                + pl.col("address").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"[^A-Z0-9]", "")
            )
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                bbl_expr.alias("registration_id"),
                pl.col("address").cast(pl.Utf8).str.strip_chars().alias("address"),
                pl.col("zip_code").cast(pl.Utf8).str.strip_chars().alias("zip"),
            )
            .unique(subset=["bbl"])
            .with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
                pl.lit("detroit").alias("jurisdiction"),
            )
        )

        return props.select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts — from property_owner_name
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "detroit_blight.parquet")

        raw = raw.filter(
            pl.col("property_owner_name").is_not_null()
            & (pl.col("property_owner_name").cast(pl.Utf8).str.strip_chars() != "")
        )

        bbl_expr = (
            pl.when(pl.col("parcel_id").is_not_null() & (pl.col("parcel_id").cast(pl.Utf8).str.strip_chars() != ""))
            .then(pl.lit("det-") + pl.col("parcel_id").cast(pl.Utf8).str.strip_chars())
            .otherwise(
                pl.lit("det-addr-")
                + pl.col("address").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"[^A-Z0-9]", "")
            )
        )

        return (
            raw.select(
                bbl_expr.alias("registration_id"),
                pl.lit(None, dtype=pl.Utf8).alias("first_name"),
                pl.lit(None, dtype=pl.Utf8).alias("last_name"),
                pl.col("property_owner_name").cast(pl.Utf8).str.strip_chars().alias("business_name"),
                pl.col("property_owner_street_number").cast(pl.Utf8).str.strip_chars().alias("business_house_number"),
                (
                    pl.col("property_owner_street_name").cast(pl.Utf8).fill_null("")
                    + pl.lit(", ")
                    + pl.col("property_owner_city").cast(pl.Utf8).fill_null("")
                    + pl.lit(" ")
                    + pl.col("property_owner_state").cast(pl.Utf8).fill_null("")
                    + pl.lit(" ")
                    + pl.col("property_owner_zip_code").cast(pl.Utf8).fill_null("")
                ).str.strip_chars().alias("business_street"),
                pl.lit("detroit").alias("jurisdiction"),
            )
            .unique(subset=["registration_id", "business_name"])
        )


# ======================================================================
# Helpers
# ======================================================================

def _matches_any(expr: pl.Expr, keywords: list[str]) -> pl.Expr:
    """Build an OR expression matching any keyword in a lowercased string column."""
    result = expr.str.contains(keywords[0])
    for kw in keywords[1:]:
        result = result | expr.str.contains(kw)
    return result
