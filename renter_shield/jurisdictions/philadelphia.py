"""Philadelphia jurisdiction adapter — L&I Violations + OPA Properties.

Data sources:
  - L&I Violations (Carto: violations table): Code enforcement violations
    with violationcode, violationstatus, caseprioritydesc, opa_account_num,
    opa_owner, and address.  Owner name is embedded in violations.
  - OPA Properties (Carto: opa_properties_public): Property assessment data
    with owner_1, owner_2, parcel_number, location, mailing addresses,
    year_built, total_livable_area, category_code_description.

Philadelphia uses the International Property Maintenance Code (IPMC) and
assigns case priorities (EMERGENCY, IMMEDIATE, STANDARD) which map to our
severity tiers.  The opa_account_num links violations to OPA properties.
"""

from __future__ import annotations

import urllib.request
import urllib.parse
from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Carto SQL API base
_CARTO_BASE = "https://phl.carto.com/api/v2/sql"

# Case priority → universal severity tier
_PRIORITY_MAP = {
    "EMERGENCY": 1,     # Critical / Immediately hazardous
    "IMMEDIATE": 1,
    "PRIORITY": 2,      # Serious
    "STANDARD": 2,
    "NON-PRIORITY": 3,  # Minor
}

# Violation code prefixes that indicate critical issues (IPMC chapters)
_CRITICAL_CODE_PREFIXES = [
    "PM15-108",   # Unsafe structures
    "PM15-109",   # Emergency measures
    "PM15-302.5", # Rodent harborage
    "PM15-505",   # Electrical hazards
]


def _fetch_carto_csv(query: str, out_path: Path) -> None:
    """Download a Carto SQL query result as CSV and save to disk."""
    params = urllib.parse.urlencode({"q": query, "format": "csv"})
    url = f"{_CARTO_BASE}?{params}"
    print(f"  GET {url[:120]}…")
    urllib.request.urlretrieve(url, out_path)


class PhiladelphiaAdapter(JurisdictionAdapter):
    jurisdiction_code = "philadelphia"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Violations — 2023+ only
        print("[philadelphia] downloading violations…")
        viol_csv = self.data_dir / "philly_violations.csv"
        _fetch_carto_csv(
            "SELECT violationnumber, opa_account_num, violationdate, "
            "violationcode, violationcodetitle, violationstatus, "
            "caseprioritydesc, casestatus, address, opa_owner, zip "
            "FROM violations "
            f"WHERE violationdate >= '{MIN_DATE}'",
            viol_csv,
        )
        df = pl.read_csv(viol_csv, infer_schema=False)
        out = self.data_dir / "philly_violations.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        viol_csv.unlink()
        print(f"[philadelphia] saved {len(df)} violation rows → {out}")

        # OPA Properties (residential only — category_code 1-4)
        print("[philadelphia] downloading OPA properties…")
        opa_csv = self.data_dir / "philly_opa_properties.csv"
        _fetch_carto_csv(
            "SELECT parcel_number, owner_1, owner_2, location, "
            "house_number, street_name, street_designation, "
            "mailing_street, mailing_city_state, "
            "year_built, total_livable_area, number_of_bedrooms, "
            "category_code, category_code_description, "
            "number_stories, zip_code "
            "FROM opa_properties_public "
            "WHERE category_code IN ('1','2','3','4')",
            opa_csv,
        )
        df = pl.read_csv(opa_csv, infer_schema=False)
        out = self.data_dir / "philly_opa_properties.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        opa_csv.unlink()
        print(f"[philadelphia] saved {len(df)} OPA rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "philly_violations.parquet")

        # Severity: map from caseprioritydesc + code heuristics
        severity_expr = (
            pl.when(pl.col("caseprioritydesc").is_in(["EMERGENCY", "IMMEDIATE"]))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(
                pl.col("violationcode").str.starts_with("PM15-108")
                | pl.col("violationcode").str.starts_with("PM15-109")
                | pl.col("violationcode").str.starts_with("PM15-302.5")
                | pl.col("violationcode").str.starts_with("PM15-505")
            )
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("caseprioritydesc").is_in(["PRIORITY", "STANDARD"]))
            .then(pl.lit(2, dtype=pl.Int8))
            .otherwise(pl.lit(3, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(
                (pl.col("violationstatus") == "COMPLIED")
                | (pl.col("casestatus") == "CLOSED")
            )
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("violationnumber").cast(pl.Utf8).alias("violation_id"),
            (pl.lit("phl-") + pl.col("opa_account_num").cast(pl.Utf8)).alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("violationdate").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("philadelphia").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "philly_opa_properties.parquet")

        return raw.select(
            (pl.lit("phl-") + pl.col("parcel_number").cast(pl.Utf8)).alias("bbl"),
            pl.col("parcel_number").cast(pl.Utf8).alias("registration_id"),
            # Philly doesn't have a unit count field; approximate from
            # number_of_bedrooms (better than defaulting to 1 for multi-family)
            pl.col("number_of_bedrooms").cast(pl.Float64, strict=False)
                .fill_null(1.0)
                .clip(lower_bound=1.0)
                .alias("units_residential"),
            pl.col("year_built").cast(pl.Utf8).alias("year_built"),
            pl.col("location").alias("address"),
            pl.lit("philadelphia").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_contacts  (synthesized from OPA owner fields + violation opa_owner)
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        # Primary source: OPA properties (owner_1, owner_2, mailing address)
        opa = pl.scan_parquet(self.data_dir / "philly_opa_properties.parquet")

        owner1 = opa.select(
            pl.col("parcel_number").cast(pl.Utf8).alias("registration_id"),
            pl.col("owner_1").str.to_uppercase().str.strip_chars().alias("_owner"),
            pl.col("house_number").cast(pl.Utf8).alias("business_house_number"),
            pl.col("street_name").cast(pl.Utf8).alias("business_street"),
        ).filter(pl.col("_owner").is_not_null())

        # Split owner name: Philly uses "LAST FIRST" or "COMPANY NAME" format
        contacts = owner1.with_columns(
            # Heuristic: take last word as first name, rest as last name
            pl.col("_owner").str.split(" ").list.last().alias("first_name"),
            pl.col("_owner").str.replace(r"\s+\S+$", "").alias("last_name"),
            pl.col("_owner").alias("business_name"),
            pl.lit("philadelphia").alias("jurisdiction"),
        ).select(
            "registration_id", "first_name", "last_name",
            "business_name", "business_house_number", "business_street",
            "jurisdiction",
        )

        return contacts
