"""San Francisco jurisdiction adapter — DBI Complaints & Violations.

Data sources:
  - DBI Complaints (nbtm-fbw5): ~510K complaints from the Department of
    Building Inspection covering code violations, unsafe buildings, work
    without permits, and nuisance complaints.  Has: complaint_number,
    item_sequence_number, date_filed, block, lot, address fields, status
    ("active" / "not active"), nov_category_description, nov_item_description.
  - Assessor Historical Secured Property Tax Rolls (wv5m-vpq2): ~212K
    parcels per roll year.  Provides year_property_built, number_of_units,
    and other property characteristics linked by block + lot.  Does NOT
    contain owner name or mailing address.

SF does not publish a public landlord/owner registry.  Contacts are returned
as an empty frame with the correct schema so the pipeline can concatenate
without errors.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Socrata pagination
_SOCRATA_PAGE_SIZE = 50_000
_SOCRATA_TIMEOUT = 120
_SOCRATA_RETRIES = 3


def _paginated_socrata_get(client, dataset_id: str, *, where: str | None = None,
                           select: str | None = None,
                           page_size: int = _SOCRATA_PAGE_SIZE) -> list[dict]:
    """Fetch all rows from a Socrata dataset using offset pagination."""
    import time
    client.timeout = _SOCRATA_TIMEOUT
    all_rows: list[dict] = []
    offset = 0
    kwargs: dict = {"limit": page_size, "order": ":id"}
    if where:
        kwargs["where"] = where
    if select:
        kwargs["select"] = select
    while True:
        kwargs["offset"] = offset
        for attempt in range(1, _SOCRATA_RETRIES + 1):
            try:
                batch = client.get(dataset_id, **kwargs)
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


# Socrata dataset identifiers on data.sfgov.org
_COMPLAINTS_ID = "nbtm-fbw5"
_ASSESSOR_ID = "wv5m-vpq2"

# nov_category_description keywords → severity tier
_CRITICAL_KEYWORDS = [
    "fire", "unsafe", "collapse", "imminent", "hazard", "emergency",
    "smoke detector", "carbon monoxide", "lead", "structural",
]
_SERIOUS_KEYWORDS = [
    "plumbing", "electrical", "building section", "interior surfaces",
    "elevator", "boiler",
]
_MINOR_KEYWORDS = [
    "sign", "permit", "registration", "posted", "work without",
    "nuisance", "noise",
]


class SFAdapter(JurisdictionAdapter):
    jurisdiction_code = "sf"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        client = Socrata("data.sfgov.org", None)

        print("[sf] downloading complaints (paginated)…")
        rows = _paginated_socrata_get(
            client, _COMPLAINTS_ID,
            where=f"date_filed >= '{MIN_DATE}'",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "sf_complaints.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[sf] saved {len(df)} rows → {out}")

        # Assessor roll — most recent year only (property characteristics)
        print("[sf] downloading assessor secured roll (paginated)…")
        rows = _paginated_socrata_get(
            client, _ASSESSOR_ID,
            where="closed_roll_year = 2024",
            select="block,lot,year_property_built,number_of_units,"
                   "number_of_bedrooms,number_of_rooms,number_of_stories,"
                   "property_area,use_code,use_definition,property_location,"
                   "assessor_neighborhood",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "sf_assessor.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[sf] saved {len(df)} assessor rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "sf_complaints.parquet")

        raw = raw.filter(pl.col("date_filed").is_not_null())

        # BBL: "sf-" + block + lot (SF Assessor block/lot system)
        bbl_expr = (
            pl.lit("sf-")
            + pl.col("block").cast(pl.Utf8).str.strip_chars()
            + pl.col("lot").cast(pl.Utf8).str.strip_chars()
        )

        # Violation ID: primary_key (unique per complaint row)
        vid_expr = pl.col("primary_key").cast(pl.Utf8)

        # Severity by code_violation_desc + unsafe_building flag keyword matching
        desc_lower = pl.col("code_violation_desc").str.to_lowercase().fill_null("")
        unsafe = pl.col("unsafe_building").str.to_lowercase().fill_null("")
        combined_desc = desc_lower + pl.lit(" ") + unsafe
        severity_expr = (
            pl.when(
                combined_desc.str.contains("|".join(_CRITICAL_KEYWORDS))
                | (unsafe == "true")
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                combined_desc.str.contains("|".join(_SERIOUS_KEYWORDS))
            ).then(pl.lit(2, dtype=pl.Int8))
            .when(
                combined_desc.str.contains("|".join(_MINOR_KEYWORDS))
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(pl.col("status").str.to_lowercase() == "active")
            .then(pl.lit("open"))
            .otherwise(pl.lit("closed"))
        )

        return raw.select(
            vid_expr.alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("date_filed").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("sf").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties (enriched from assessor secured roll when available)
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "sf_complaints.parquet")

        bbl_expr = (
            pl.lit("sf-")
            + pl.col("block").cast(pl.Utf8).str.strip_chars()
            + pl.col("lot").cast(pl.Utf8).str.strip_chars()
        )

        addr_expr = (
            pl.col("street_number").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("street_name").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("street_suffix").cast(pl.Utf8).fill_null("")
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                bbl_expr.alias("registration_id"),
                addr_expr.str.strip_chars().alias("address"),
            )
            .unique(subset=["bbl"])
        )

        # Enrich from assessor data if downloaded
        assessor_path = self.data_dir / "sf_assessor.parquet"
        if assessor_path.exists():
            assessor = pl.scan_parquet(assessor_path)
            assessor_bbl = (
                pl.lit("sf-")
                + pl.col("block").cast(pl.Utf8).str.strip_chars()
                + pl.col("lot").cast(pl.Utf8).str.strip_chars()
            )
            assessor = (
                assessor.select(
                    assessor_bbl.alias("bbl"),
                    pl.col("number_of_units").cast(pl.Float64, strict=False).alias("units_residential"),
                    pl.col("year_property_built").cast(pl.Utf8).alias("year_built"),
                )
                .unique(subset=["bbl"])
            )
            props = props.join(assessor, on="bbl", how="left")
        else:
            props = props.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            )

        return props.with_columns(
            pl.lit("sf").alias("jurisdiction"),
        ).select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts (empty — SF assessor data does not include owner names)
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
