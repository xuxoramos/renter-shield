"""Austin jurisdiction adapter — Austin Code Complaint Cases.

Data source (Socrata, data.austintexas.gov):
  - Austin Code Complaint Cases (6wtj-zbtb): ~82K cases since 2022
    Fields: case_id, case_type, description, status, opened_date,
    closed_date, parcelid, address, house_number, street_name, city,
    state, zip_code, priority (1-5), latitude, longitude,
    repeatoffenderrelated, shorttermrentalrelated.

Description values and severity mapping rationale:
  - "Structure Condition Violation(s)": habitability / structural
    defects — Tier 2 (Serious)
  - "Work Without Permit": illegal construction / unpermitted work
    — Tier 2 (Serious)
  - "Property Abatement": overgrown lots, debris, exterior maintenance
    — Tier 3 (Minor)
  - "Land Use Violation(s)": zoning / use violations
    — Tier 3 (Minor)
  - "Unknown" / others → Tier 3 (Minor) default

Limitations:
  - **No owner data** — Austin Code does not publish property owner
    information.  Properties are keyed by PARCELID (Travis County
    Appraisal District parcel number).
  - All records have case_type="Complaints"; the DESCRIPTION field
    provides the actual violation category.
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Socrata dataset identifier on data.austintexas.gov
_DATASET_ID = "6wtj-zbtb"

# Pagination / retry settings
_SOCRATA_PAGE_SIZE = 50_000
_SOCRATA_TIMEOUT = 120
_SOCRATA_RETRIES = 3

# Description → severity tier mapping
# Tier 2 = confirmed structural / safety issues
# Tier 3 = maintenance / zoning (lower-risk)
_TIER2_DESCRIPTIONS = ["structure condition", "work without permit"]
_TIER3_DESCRIPTIONS = ["property abatement", "land use"]


def _paginated_get(client, dataset_id: str, *, where: str | None = None,
                   page_size: int = _SOCRATA_PAGE_SIZE) -> list[dict]:
    """Fetch all rows from a Socrata dataset using offset pagination."""
    client.timeout = _SOCRATA_TIMEOUT
    all_rows: list[dict] = []
    offset = 0
    while True:
        for attempt in range(1, _SOCRATA_RETRIES + 1):
            try:
                batch = client.get(
                    dataset_id,
                    where=where,
                    limit=page_size,
                    offset=offset,
                    order=":id",
                )
                break
            except Exception:
                if attempt == _SOCRATA_RETRIES:
                    raise
                wait = 2 ** attempt
                print(f"  retry {attempt}/{_SOCRATA_RETRIES} in {wait}s…")
                time.sleep(wait)
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  fetched {len(all_rows)} rows so far…")
        if len(batch) < page_size:
            break
        offset += len(batch)
    return all_rows


class AustinAdapter(JurisdictionAdapter):
    jurisdiction_code = "austin"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        self.data_dir.mkdir(parents=True, exist_ok=True)
        client = Socrata("data.austintexas.gov", None)

        where = f"opened_date >= '{MIN_DATE}'"

        print("[austin] downloading code complaint cases (paginated)…")
        rows = _paginated_get(client, _DATASET_ID, where=where)
        print(f"[austin] total rows: {len(rows)}")

        if not rows:
            print("[austin] WARNING: no rows returned")
            return

        df = pl.DataFrame(rows)
        out = self.data_dir / "austin_cases.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[austin] saved {len(df)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "austin_cases.parquet")

        raw = raw.filter(pl.col("opened_date").is_not_null())

        # BBL from parcel ID: prefix with "austin-"
        bbl_expr = (
            pl.lit("austin-")
            + pl.col("parcelid").cast(pl.Utf8).str.strip_chars()
        )

        # Severity mapping by description keyword matching
        desc_lower = pl.col("description").cast(pl.Utf8).str.to_lowercase()
        severity_expr = (
            pl.when(
                desc_lower.str.contains("structure condition")
                | desc_lower.str.contains("work without permit")
            )
            .then(pl.lit(2, dtype=pl.Int8))
            .otherwise(pl.lit(3, dtype=pl.Int8))
        )

        # Status mapping
        status_expr = (
            pl.when(pl.col("status").cast(pl.Utf8).str.to_lowercase().str.contains("closed"))
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("case_id").cast(pl.Utf8).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("opened_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("austin").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "austin_cases.parquet")

        bbl_expr = (
            pl.lit("austin-")
            + pl.col("parcelid").cast(pl.Utf8).str.strip_chars()
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
                pl.lit("austin").alias("jurisdiction"),
            )
        )

        return props.select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts (empty — no owner data)
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            schema={
                "registration_id": pl.Utf8,
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "business_name": pl.Utf8,
                "business_house_number": pl.Utf8,
                "business_street": pl.Utf8,
                "jurisdiction": pl.Utf8,
            }
        )
