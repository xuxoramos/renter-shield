"""Miami-Dade County jurisdiction adapter — Code Compliance + Building Violations.

Data sources (ArcGIS Feature Service, gis-mdc.opendata.arcgis.com):
  - Code Compliance Violations (CCVIOL): ~183K open housing code violations
    in unincorporated Miami-Dade. Fields: CASE_NUM, CASE_DATE, CASE_STATUS,
    STAT_DESC, ADDRESS, FOLIO, PROBLEM, PROBLEM_DESC, LAST_ACTV.
  - Building Violations: ~76K building violation cases.
    Fields: CASE_NUM, CASE_TYPE, FOLIO, PROP_ADDR, USBILLDATE,
    VIOL_NAME (violator/owner name), OPEN_DATE, CLOSED_DATE.

PROBLEM_DESC values and severity mapping rationale (Code Compliance):
  - "Construction Performed without Required Permit" → Tier 1 (Critical)
  - "Structure Maintenance" keywords → Tier 2 (Serious)
  - "Unauthorized Use", "Foreclosure", "Failure to Obtain" → Tier 2
  - "Junk/Trash/Overgrowth", "Signs", "Graffiti", vehicle storage,
    boat storage → Tier 3 (Minor)

Building Violation CASE_TYPE severity mapping:
  - "Unsafe Structure" → Tier 1 (Critical)
  - "Boilers" → Tier 1 (Critical)
  - "All Other Code Violations", "Expired Permit" → Tier 2 (Serious)

Owner data:
  - Building Violations has VIOL_NAME (violator/property owner name).
    Code Compliance does NOT have owner data.
  - FOLIO is the Miami-Dade property folio number (parcel ID).
"""

from __future__ import annotations

import json
import time
import urllib.request

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# ArcGIS REST endpoints
_CCVIOL_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services"
    "/CCVIOL_gdb/FeatureServer/0"
)
_BUILDING_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services"
    "/BuildingViolation_gdb/FeatureServer/0"
)

# Pagination settings (ArcGIS maxRecordCount = 2000 for these services)
_PAGE_SIZE = 2000
_TIMEOUT = 120
_RETRIES = 3

# ---- Severity mapping for Code Compliance PROBLEM_DESC ----
# Keywords matched case-insensitively against PROBLEM_DESC
_CC_CRITICAL_KEYWORDS = [
    "construction performed without",  # unpermitted construction
]
_CC_SERIOUS_KEYWORDS = [
    "structure maintenance",   # structural upkeep
    "unauthorized use",        # illegal occupancy / use
    "foreclosure",             # foreclosure registry / failure to register
    "failure to obtain",       # missing certificate of use
    "pool fence",              # safety barrier
    "pool maintenance",        # standing water / health hazard
    "setback violations",      # structural encroachments
]
# Everything else defaults to Tier 3 (junk/trash, vehicles, signs, etc.)

# ---- Severity mapping for Building Violations CASE_TYPE ----
_BV_CRITICAL_TYPES = ["unsafe structure", "boilers"]
_BV_SERIOUS_TYPES = ["all other code violations", "expired permit"]


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
        print(f"  fetched {len(all_features)} features…")
        if not data.get("exceededTransferLimit", False) and len(features) < page_size:
            break
        offset += len(features)
    return all_features


class MiamiAdapter(JurisdictionAdapter):
    jurisdiction_code = "miami"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Code Compliance Violations
        # CASE_DATE is esriFieldTypeDate — use TIMESTAMP literal
        cc_where = f"CASE_DATE >= TIMESTAMP '{MIN_DATE}'"
        print("[miami] downloading code compliance violations (ArcGIS paginated)…")
        cc_rows = _arcgis_paginated_get(_CCVIOL_URL, where=cc_where)
        print(f"[miami] code compliance: {len(cc_rows)} rows")

        if cc_rows:
            df_cc = pl.DataFrame(cc_rows)
            out = self.data_dir / "miami_ccviol.parquet"
            df_cc.write_parquet(out, compression="zstd", compression_level=3)
            print(f"[miami] saved {len(df_cc)} rows → {out}")

        # Building Violations
        # OPEN_DATE is esriFieldTypeDate — use TIMESTAMP literal
        bv_where = f"OPEN_DATE >= TIMESTAMP '{MIN_DATE}'"
        print("[miami] downloading building violations (ArcGIS paginated)…")
        bv_rows = _arcgis_paginated_get(_BUILDING_URL, where=bv_where)
        print(f"[miami] building violations: {len(bv_rows)} rows")

        if bv_rows:
            df_bv = pl.DataFrame(bv_rows)
            out = self.data_dir / "miami_building.parquet"
            df_bv.write_parquet(out, compression="zstd", compression_level=3)
            print(f"[miami] saved {len(df_bv)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        frames = []

        # Code Compliance
        cc_path = self.data_dir / "miami_ccviol.parquet"
        if cc_path.exists():
            cc = pl.scan_parquet(cc_path)
            # Filter out records with empty FOLIO (no parcel linkage)
            cc = cc.filter(
                pl.col("FOLIO").is_not_null()
                & (pl.col("FOLIO").cast(pl.Utf8).str.strip_chars() != "")
            )
            desc_lower = pl.col("PROBLEM_DESC").cast(pl.Utf8).str.to_lowercase()
            severity_cc = (
                pl.when(_matches_any(desc_lower, _CC_CRITICAL_KEYWORDS))
                .then(pl.lit(1, dtype=pl.Int8))
                .when(_matches_any(desc_lower, _CC_SERIOUS_KEYWORDS))
                .then(pl.lit(2, dtype=pl.Int8))
                .otherwise(pl.lit(3, dtype=pl.Int8))
            )
            status_cc = (
                pl.when(pl.col("CASE_STATUS").cast(pl.Utf8).str.to_uppercase().str.strip_chars() == "CL")
                .then(pl.lit("closed"))
                .otherwise(pl.lit("open"))
            )
            cc_violations = cc.select(
                (pl.lit("CC-") + pl.col("CASE_NUM").cast(pl.Utf8).str.strip_chars()).alias("violation_id"),
                (pl.lit("miami-") + pl.col("FOLIO").cast(pl.Utf8).str.strip_chars()).alias("bbl"),
                severity_cc.alias("severity_tier"),
                status_cc.alias("status"),
                _epoch_to_date("CASE_DATE").alias("inspection_date"),
                pl.lit("miami").alias("jurisdiction"),
            )
            frames.append(cc_violations)

        # Building Violations
        bv_path = self.data_dir / "miami_building.parquet"
        if bv_path.exists():
            bv = pl.scan_parquet(bv_path)
            type_lower = pl.col("CASE_TYPE").cast(pl.Utf8).str.to_lowercase()
            severity_bv = (
                pl.when(type_lower.is_in(_BV_CRITICAL_TYPES))
                .then(pl.lit(1, dtype=pl.Int8))
                .when(type_lower.is_in(_BV_SERIOUS_TYPES))
                .then(pl.lit(2, dtype=pl.Int8))
                .otherwise(pl.lit(3, dtype=pl.Int8))
            )
            status_bv = (
                pl.when(pl.col("CLOSED_DATE").is_not_null())
                .then(pl.lit("closed"))
                .otherwise(pl.lit("open"))
            )
            bv_violations = bv.select(
                (pl.lit("BV-") + pl.col("CASE_NUM").cast(pl.Utf8).str.strip_chars()).alias("violation_id"),
                (pl.lit("miami-") + pl.col("FOLIO").cast(pl.Utf8).str.strip_chars()).alias("bbl"),
                severity_bv.alias("severity_tier"),
                status_bv.alias("status"),
                _epoch_to_date("OPEN_DATE").alias("inspection_date"),
                pl.lit("miami").alias("jurisdiction"),
            )
            frames.append(bv_violations)

        if not frames:
            return pl.LazyFrame(schema={
                "violation_id": pl.Utf8, "bbl": pl.Utf8,
                "severity_tier": pl.Int8, "status": pl.Utf8,
                "inspection_date": pl.Date, "jurisdiction": pl.Utf8,
            })

        return pl.concat(frames)

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        frames = []

        cc_path = self.data_dir / "miami_ccviol.parquet"
        if cc_path.exists():
            cc = pl.scan_parquet(cc_path)
            frames.append(cc.select(
                (pl.lit("miami-") + pl.col("FOLIO").cast(pl.Utf8).str.strip_chars()).alias("bbl"),
                pl.col("FOLIO").cast(pl.Utf8).str.strip_chars().alias("registration_id"),
                pl.col("ADDRESS").cast(pl.Utf8).str.strip_chars().alias("address"),
            ))

        bv_path = self.data_dir / "miami_building.parquet"
        if bv_path.exists():
            bv = pl.scan_parquet(bv_path)
            frames.append(bv.select(
                (pl.lit("miami-") + pl.col("FOLIO").cast(pl.Utf8).str.strip_chars()).alias("bbl"),
                pl.col("FOLIO").cast(pl.Utf8).str.strip_chars().alias("registration_id"),
                pl.col("PROP_ADDR").cast(pl.Utf8).str.strip_chars().alias("address"),
            ))

        if not frames:
            return _empty_properties()

        props = (
            pl.concat(frames)
            .unique(subset=["bbl"])
            .with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
                pl.lit("miami").alias("jurisdiction"),
            )
        )
        return props.select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts — from Building Violations VIOL_NAME
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        bv_path = self.data_dir / "miami_building.parquet"
        if not bv_path.exists():
            return _empty_contacts()

        bv = pl.scan_parquet(bv_path)
        bv = bv.filter(pl.col("VIOL_NAME").is_not_null())

        return bv.select(
            pl.col("FOLIO").cast(pl.Utf8).str.strip_chars().alias("registration_id"),
            pl.lit(None, dtype=pl.Utf8).alias("first_name"),
            pl.lit(None, dtype=pl.Utf8).alias("last_name"),
            pl.col("VIOL_NAME").cast(pl.Utf8).str.strip_chars().alias("business_name"),
            pl.lit(None, dtype=pl.Utf8).alias("business_house_number"),
            pl.lit(None, dtype=pl.Utf8).alias("business_street"),
            pl.lit("miami").alias("jurisdiction"),
        ).unique(subset=["registration_id", "business_name"])


# ======================================================================
# Helpers
# ======================================================================

def _epoch_to_date(col_name: str) -> pl.Expr:
    """Convert an ArcGIS epoch-ms column to a pl.Date."""
    return pl.from_epoch(pl.col(col_name).cast(pl.Int64), time_unit="ms").dt.date()


def _matches_any(expr: pl.Expr, keywords: list[str]) -> pl.Expr:
    """Build an OR expression matching any keyword in a lowercased string column."""
    result = expr.str.contains(keywords[0])
    for kw in keywords[1:]:
        result = result | expr.str.contains(kw)
    return result


def _empty_properties() -> pl.LazyFrame:
    return pl.LazyFrame(schema={
        "bbl": pl.Utf8, "registration_id": pl.Utf8,
        "units_residential": pl.Float64, "year_built": pl.Utf8,
        "address": pl.Utf8, "jurisdiction": pl.Utf8,
    })


def _empty_contacts() -> pl.LazyFrame:
    return pl.LazyFrame(schema={
        "registration_id": pl.Utf8, "first_name": pl.Utf8,
        "last_name": pl.Utf8, "business_name": pl.Utf8,
        "business_house_number": pl.Utf8, "business_street": pl.Utf8,
        "jurisdiction": pl.Utf8,
    })
