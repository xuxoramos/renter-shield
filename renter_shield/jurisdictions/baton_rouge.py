"""Baton Rouge jurisdiction adapter — 311 Code/Blight complaints + Tax Roll.

Data sources (all Socrata, data.brla.gov):
  - 311 Citizen Requests (7ixm-mnvx): ~987K total; we filter to
    parenttype IN ('BUILDING CODE/ZONING VIOLATIONS', 'BLIGHTED PROPERTIES').
    Has: id, createdate, closeddate, statusdesc, typename, comments,
    streetaddress, latitude, longitude.
  - EBRP Tax Roll (myfc-nh6n): ~4M rows (multi-year, multi-unit-type).
    Has: assessment_no, taxpayer_name, taxpayer_addr_1, taxpayer_addr_2,
    legal_description.
  - Property Information (re5c-hrw9): ~222K address-to-lot lookup.
    Has: lot_id, full_address, subdivision, existing_land_use.
  - EBR Building Permits (7fq7-8j7r): 141K permits.
    Has: ownername, address, permittype, issueddate.

Join strategy:
  BBL = "br-{lot_id}" sourced from Property Information. 311 violations
  join via normalized streetaddress → full_address. Owner data comes from
  the Tax Roll (taxpayer_name) keyed by assessment_no extracted from the
  legal_description ward/lot path matched to Property Info lot_id.

  Fallback owner: Building Permits ownername matched by address.
"""

from __future__ import annotations

import time

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Socrata dataset identifiers
_311_ID = "7ixm-mnvx"
_TAX_ROLL_ID = "myfc-nh6n"
_PROPERTY_INFO_ID = "re5c-hrw9"
_PERMITS_ID = "7fq7-8j7r"

# Only these 311 parent types represent housing code violations
_RELEVANT_PARENT_TYPES = (
    "BUILDING CODE/ZONING VIOLATIONS",
    "BLIGHTED PROPERTIES",
)

# Socrata pagination settings
_PAGE_SIZE = 50_000
_TIMEOUT = 120
_RETRIES = 3

# ──────────────────────────────────────────────────────────────────────────────
# Severity mapping: typename → universal tier (1=critical … 4=informational)
# ──────────────────────────────────────────────────────────────────────────────
# Tier 1 — Critical: immediate hazard to life/safety
_TIER1_KEYWORDS = [
    "condemned",
    "torn down",
]
# Tier 2 — Serious: habitability impact
_TIER2_KEYWORDS = [
    "missing windows",
    "missing doors",
    "building code violations",
    "swimming pool",       # dirty or unfenced pool = drowning risk
]
# Tier 3 — Moderate: code/zoning non-compliance
_TIER3_KEYWORDS = [
    "zoning code violation",
    "sign without a permit",
    "illegal sign",
    "sidewalk",
    "waste ordinance",
]
# Everything else (tall grass, junk, debris, abandoned vehicle) → Tier 4


def _paginated_socrata_get(
    client, dataset_id: str, *, where: str | None = None
) -> list[dict]:
    """Fetch all rows from a Socrata dataset with retry + pagination."""
    client.timeout = _TIMEOUT
    all_rows: list[dict] = []
    offset = 0
    while True:
        for attempt in range(1, _RETRIES + 1):
            try:
                batch = client.get(
                    dataset_id,
                    where=where,
                    limit=_PAGE_SIZE,
                    offset=offset,
                    order=":id",
                )
                break
            except Exception:
                if attempt == _RETRIES:
                    raise
                time.sleep(2 ** attempt)
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  fetched {len(all_rows)} rows so far…")
        if len(batch) < _PAGE_SIZE:
            break
        offset += len(batch)
    return all_rows


class BatonRougeAdapter(JurisdictionAdapter):
    jurisdiction_code = "baton_rouge"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        client = Socrata("data.brla.gov", None)

        # 1. 311 violations (code + blight only, since MIN_DATE)
        where = (
            f"parenttype in('{_RELEVANT_PARENT_TYPES[0]}','{_RELEVANT_PARENT_TYPES[1]}') "
            f"AND createdate >= '{MIN_DATE}'"
        )
        print("[baton_rouge] downloading 311 code/blight complaints…")
        rows = _paginated_socrata_get(client, _311_ID, where=where)
        df = pl.DataFrame(rows)
        out = self.data_dir / "baton_rouge_violations.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[baton_rouge] saved {len(df)} violation rows → {out}")

        # 2. Property Information (address → lot_id mapping)
        print("[baton_rouge] downloading property info…")
        rows = _paginated_socrata_get(client, _PROPERTY_INFO_ID)
        df = pl.DataFrame(rows)
        out = self.data_dir / "baton_rouge_properties.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[baton_rouge] saved {len(df)} property rows → {out}")

        # 3. Tax Roll (owner names — only most recent tax year)
        print("[baton_rouge] downloading tax roll (latest year)…")
        rows = _paginated_socrata_get(
            client, _TAX_ROLL_ID,
            where="tax_year = '2025' AND unit_type = 'IMPROVEMENT'",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "baton_rouge_tax_roll.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[baton_rouge] saved {len(df)} tax roll rows → {out}")

        # 4. Building Permits (fallback owner via ownername field)
        print("[baton_rouge] downloading building permits…")
        rows = _paginated_socrata_get(
            client, _PERMITS_ID,
            where=f"issueddate >= '{MIN_DATE}'",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "baton_rouge_permits.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[baton_rouge] saved {len(df)} permit rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "baton_rouge_violations.parquet")

        # Build address lookup: normalized streetaddress → lot_id (BBL)
        props = pl.scan_parquet(self.data_dir / "baton_rouge_properties.parquet")
        addr_to_bbl = (
            props.select(
                pl.col("full_address")
                .str.to_uppercase()
                .str.strip_chars()
                .alias("_addr_norm"),
                (pl.lit("br-") + pl.col("lot_id").cast(pl.Utf8)).alias("bbl"),
            )
            .unique(subset=["_addr_norm"])
        )

        # Severity tier from typename keyword matching
        tier1_pat = "|".join(_TIER1_KEYWORDS)
        tier2_pat = "|".join(_TIER2_KEYWORDS)
        tier3_pat = "|".join(_TIER3_KEYWORDS)

        severity_expr = (
            pl.when(
                pl.col("typename").str.to_lowercase().str.contains(tier1_pat)
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                pl.col("typename").str.to_lowercase().str.contains(tier2_pat)
            ).then(pl.lit(2, dtype=pl.Int8))
            .when(
                pl.col("typename").str.to_lowercase().str.contains(tier3_pat)
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(4, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(pl.col("statusdesc") == "CLOSED")
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        violations = raw.with_columns(
            pl.col("streetaddress")
            .str.to_uppercase()
            .str.strip_chars()
            .alias("_addr_norm"),
        ).join(addr_to_bbl, on="_addr_norm", how="left").select(
            pl.col("id").cast(pl.Utf8).alias("violation_id"),
            pl.col("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("createdate").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("baton_rouge").alias("jurisdiction"),
        )

        return violations

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        props = pl.scan_parquet(self.data_dir / "baton_rouge_properties.parquet")

        return props.select(
            (pl.lit("br-") + pl.col("lot_id").cast(pl.Utf8)).alias("bbl"),
            pl.col("lot_id").cast(pl.Utf8).alias("registration_id"),
            pl.lit(1.0).alias("units_residential"),  # not available in source
            pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            pl.col("full_address").alias("address"),
            pl.lit("baton_rouge").alias("jurisdiction"),
        ).unique(subset=["bbl"])

    # ------------------------------------------------------------------
    # load_contacts
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        empty_schema = {
            "registration_id": pl.Utf8,
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "business_name": pl.Utf8,
            "business_house_number": pl.Utf8,
            "business_street": pl.Utf8,
            "jurisdiction": pl.Utf8,
        }

        tax_path = self.data_dir / "baton_rouge_tax_roll.parquet"
        props_path = self.data_dir / "baton_rouge_properties.parquet"
        if not tax_path.exists() or not props_path.exists():
            return pl.LazyFrame(schema=empty_schema)

        # Tax Roll has assessment_no (e.g. "010-1668-7") + taxpayer_name
        # Property Info has lot_id. We bridge via taxpayer_addr_1 ↔ full_address.
        tax = pl.scan_parquet(tax_path)
        props = pl.scan_parquet(props_path)

        # Normalize addresses for join
        tax_contacts = tax.select(
            pl.col("taxpayer_addr_1")
            .str.to_uppercase()
            .str.strip_chars()
            .alias("_addr_norm"),
            pl.col("taxpayer_name").str.to_uppercase().str.strip_chars().alias("_owner"),
            pl.col("assessment_no").cast(pl.Utf8).alias("_assessment_no"),
        ).unique(subset=["_addr_norm"])

        props_lookup = props.select(
            pl.col("full_address")
            .str.to_uppercase()
            .str.strip_chars()
            .alias("_addr_norm"),
            pl.col("lot_id").cast(pl.Utf8).alias("registration_id"),
        ).unique(subset=["_addr_norm"])

        # Join tax contacts to properties via address
        joined = tax_contacts.join(props_lookup, on="_addr_norm", how="inner")

        # Parse taxpayer_name: typically "LAST, FIRST" or "BUSINESS NAME LLC"
        contacts = joined.with_columns(
            pl.when(pl.col("_owner").str.contains(","))
            .then(
                pl.col("_owner").str.split(",").list.get(1, null_on_oob=True).str.strip_chars()
            )
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("first_name"),
            pl.when(pl.col("_owner").str.contains(","))
            .then(
                pl.col("_owner").str.split(",").list.get(0, null_on_oob=True).str.strip_chars()
            )
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("last_name"),
        ).select(
            "registration_id",
            "first_name",
            "last_name",
            pl.col("_owner").alias("business_name"),
            pl.lit(None, dtype=pl.Utf8).alias("business_house_number"),
            pl.col("_addr_norm").alias("business_street"),
            pl.lit("baton_rouge").alias("jurisdiction"),
        )

        return contacts
