"""HUD REAC jurisdiction adapter — Multifamily Assisted Properties inspection scores.

Data source:
  - HUD Multifamily Properties - Assisted (ArcGIS Feature Service):
    ~23,800 subsidized housing properties nationwide with REAC physical
    inspection scores, management agent contacts, risk categories, and
    property characteristics.

    Endpoint: services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/
              Multifamily_Properties_Assisted/FeatureServer/0

Unlike city-level adapters that track individual code violations, HUD REAC
provides a single inspection score (0-100) per property per inspection cycle.
The adapter converts this into a synthetic violation per property, with the
severity tier derived from the inspection score:
  - 0-59  → Tier 1 (Critical): property fails federal standards
  - 60-79 → Tier 2 (Serious): substandard conditions
  - 80-89 → Tier 3 (Minor): adequate but needs improvement
  - 90-100 → Tier 4 (Informational): passing

The management agent (MGMT_AGENT_ORG_NAME, MGMT_CONTACT_FULL_NAME) is used
as the contact, allowing correlation with city-level landlord data.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# ArcGIS Feature Service endpoint
_BASE_URL = (
    "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/"
    "Multifamily_Properties_Assisted/FeatureServer/0/query"
)

# Fields to download
_OUT_FIELDS = ",".join([
    "PROPERTY_ID",
    "PROPERTY_NAME_TEXT",
    "ADDRESS_LINE1_TEXT",
    "STD_CITY",
    "STD_ST",
    "STD_ZIP5",
    "TOTAL_UNIT_COUNT",
    "TOTAL_ASSISTED_UNIT_COUNT",
    "REAC_LAST_INSPECTION_ID",
    "REAC_LAST_INSPECTION_SCORE",
    "REAC_LAST_INSPECTION_DATE",
    "TROUBLED_CODE",
    "OPIIS_RISK_CATEGORY",
    "OPIIS_INT_RISK_SCORE",
    "MGMT_AGENT_ORG_NAME",
    "MGMT_CONTACT_FULL_NAME",
    "MGMT_CONTACT_ADDRESS_LINE1",
    "MGMT_CONTACT_CITY_NAME",
    "MGMT_CONTACT_STATE_CODE",
    "MGMT_CONTACT_ZIP_CODE",
    "CLIENT_GROUP_NAME",
    "CLIENT_GROUP_TYPE",
    "LAT",
    "LON",
])

# ArcGIS returns max 2000 per page
_PAGE_SIZE = 2000


def _fetch_all_features() -> list[dict]:
    """Page through the ArcGIS Feature Service and collect all records."""
    all_features: list[dict] = []
    offset = 0

    while True:
        params = urllib.parse.urlencode({
            "where": "REAC_LAST_INSPECTION_SCORE IS NOT NULL",
            "outFields": _OUT_FIELDS,
            "resultOffset": offset,
            "resultRecordCount": _PAGE_SIZE,
            "f": "json",
        })
        url = f"{_BASE_URL}?{params}"
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            all_features.append(f["attributes"])

        # ArcGIS signals last page when exceededTransferLimit is absent/false
        if not data.get("exceededTransferLimit", False):
            break
        offset += len(features)

    return all_features


class HUDREACAdapter(JurisdictionAdapter):
    jurisdiction_code = "hud_reac"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        print("[hud_reac] downloading Multifamily Assisted properties…")
        features = _fetch_all_features()
        df = pl.DataFrame(features)

        # Convert epoch-ms dates to ISO strings for consistent handling
        for date_col in ["REAC_LAST_INSPECTION_DATE"]:
            if date_col in df.columns:
                df = df.with_columns(
                    (pl.col(date_col).cast(pl.Int64) / 1000)
                    .cast(pl.Int64)
                    .map_elements(
                        lambda ts: None if ts is None else __import__("datetime").datetime.fromtimestamp(ts, tz=__import__("datetime").timezone.utc).strftime("%Y-%m-%d"),
                        return_dtype=pl.Utf8,
                    )
                    .alias(date_col)
                )

        out = self.data_dir / "hud_reac_multifamily.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[hud_reac] saved {len(df)} properties → {out}")

    # ------------------------------------------------------------------
    # load_violations (synthetic — one "violation" per property per score)
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "hud_reac_multifamily.parquet")

        # Filter by MIN_DATE on inspection date
        raw = raw.filter(
            pl.col("REAC_LAST_INSPECTION_DATE").is_not_null()
            & (pl.col("REAC_LAST_INSPECTION_DATE") >= MIN_DATE)
        )

        # BBL: "hud-" + PROPERTY_ID
        bbl_expr = pl.lit("hud-") + pl.col("PROPERTY_ID").cast(pl.Utf8)

        # Severity from inspection score (0-100)
        score = pl.col("REAC_LAST_INSPECTION_SCORE").cast(pl.Float64, strict=False)
        severity_expr = (
            pl.when(score < 60).then(pl.lit(1, dtype=pl.Int8))
            .when(score < 80).then(pl.lit(2, dtype=pl.Int8))
            .when(score < 90).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(4, dtype=pl.Int8))
        )

        # Status: TROUBLED_CODE "N" = not troubled (closed), others = open
        status_expr = (
            pl.when(pl.col("TROUBLED_CODE") == "N")
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            (pl.lit("reac-") + pl.col("REAC_LAST_INSPECTION_ID").cast(pl.Utf8)).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("REAC_LAST_INSPECTION_DATE").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("hud_reac").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "hud_reac_multifamily.parquet")

        bbl_expr = pl.lit("hud-") + pl.col("PROPERTY_ID").cast(pl.Utf8)

        addr_expr = (
            pl.col("ADDRESS_LINE1_TEXT").cast(pl.Utf8).fill_null("")
            + pl.lit(", ")
            + pl.col("STD_CITY").cast(pl.Utf8).fill_null("").str.strip_chars()
            + pl.lit(", ")
            + pl.col("STD_ST").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("STD_ZIP5").cast(pl.Utf8).fill_null("")
        )

        return raw.select(
            bbl_expr.alias("bbl"),
            pl.col("PROPERTY_ID").cast(pl.Utf8).alias("registration_id"),
            pl.col("TOTAL_UNIT_COUNT").cast(pl.Float64, strict=False).alias("units_residential"),
            pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            addr_expr.str.strip_chars().alias("address"),
            pl.lit("hud_reac").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_contacts (management agent info)
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "hud_reac_multifamily.parquet")

        raw = raw.filter(pl.col("MGMT_AGENT_ORG_NAME").is_not_null())

        # Split MGMT_CONTACT_FULL_NAME into first/last
        name = pl.col("MGMT_CONTACT_FULL_NAME").str.to_uppercase().str.strip_chars().fill_null("")

        return raw.select(
            pl.col("PROPERTY_ID").cast(pl.Utf8).alias("registration_id"),
            # Heuristic: first word = first name, rest = last name
            name.str.split(" ").list.first().alias("first_name"),
            name.str.replace(r"^\S+\s*", "").alias("last_name"),
            pl.col("MGMT_AGENT_ORG_NAME").str.to_uppercase().str.strip_chars().alias("business_name"),
            pl.lit(None, dtype=pl.Utf8).alias("business_house_number"),
            pl.col("MGMT_CONTACT_ADDRESS_LINE1").cast(pl.Utf8).alias("business_street"),
            pl.lit("hud_reac").alias("jurisdiction"),
        )
