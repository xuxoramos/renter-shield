"""Seattle jurisdiction adapter — SDCI Code Complaints & Violations.

Data source:
  - Code Complaints and Violations (ez4a-iug7): ~70K records since 2022
    from the Seattle Department of Construction and Inspections (SDCI).
    Covers housing code, landlord/tenant, vacant buildings, construction,
    land use, and emergency complaints/violations.  Has: recordnum,
    recordtype, recordtypemapped (Request vs Case), recordtypedesc,
    description, opendate, lastinspdate, lastinspresult, statuscurrent,
    originaladdress1, latitude, longitude.
    Does NOT have: parcel ID or owner name.

Seattle does not publish parcel-level property or owner/assessor data on its
Socrata portal (King County Assessor manages that separately and does not
offer a usable Socrata dataset).  Properties are keyed by address.  Contacts
are returned as an empty frame with the correct schema.
"""

from __future__ import annotations

import time

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Socrata dataset identifier on data.seattle.gov
_COMPLAINTS_ID = "ez4a-iug7"

# Pagination / retry settings
_SOCRATA_PAGE_SIZE = 50_000
_SOCRATA_TIMEOUT = 120
_SOCRATA_RETRIES = 3

# recordtypedesc keywords → severity tier
# sorted alphabetically within each tier; rationale in comments
_CRITICAL_KEYWORDS = [
    "carbon monoxide",  # CO hazard
    "collapse",         # structural failure
    "emergency",        # SDCI emergency classification
    "fire",             # fire hazard / fire code
    "hazard",           # generic hazard flag
    "imminent",         # imminent danger
    "lead",             # lead paint / lead hazard
    "no heat",          # habitability — heat failure
    "no hot water",     # habitability — hot water failure
    "smoke detector",   # life safety device
    "structural",       # structural issues
    "unsafe",           # unsafe conditions
    "vacate",           # emergency vacate order
]
_SERIOUS_KEYWORDS = [
    "electrical",       # electrical code
    "landlord/tenant",  # housing habitability disputes
    "plumbing",         # plumbing code
    "vacant building",  # vacant/abandoned structures
]
_MINOR_KEYWORDS = [
    "land use",         # zoning / land use compliance
    "noise",            # noise complaints
    "permit",           # permit issues
    "sign",             # signage violations
    "tree",             # tree code
    "weeds",            # overgrown vegetation
]


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


class SeattleAdapter(JurisdictionAdapter):
    jurisdiction_code = "seattle"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        self.data_dir.mkdir(parents=True, exist_ok=True)
        client = Socrata("data.seattle.gov", None)

        # All complaints and violations since MIN_DATE, no geographic filter
        print("[seattle] downloading code complaints & violations (paginated)…")
        rows = _paginated_get(
            client, _COMPLAINTS_ID,
            where=f"opendate >= '{MIN_DATE}'",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "seattle_complaints.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[seattle] saved {len(df)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "seattle_complaints.parquet")

        raw = raw.filter(pl.col("opendate").is_not_null())

        # BBL: "sea-" + address (normalized) — Seattle has no parcel ID in
        # this dataset, so we use the street address as the property key
        bbl_expr = (
            pl.lit("sea-")
            + pl.col("originaladdress1").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        )

        # Severity: combine recordtypedesc + description for keyword matching
        type_lower = pl.col("recordtypedesc").str.to_lowercase().fill_null("")
        desc_lower = pl.col("description").str.to_lowercase().fill_null("")
        status_lower = pl.col("statuscurrent").str.to_lowercase().fill_null("")
        combined = type_lower + pl.lit(" ") + desc_lower + pl.lit(" ") + status_lower

        severity_expr = (
            pl.when(
                combined.str.contains("|".join(_CRITICAL_KEYWORDS))
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                combined.str.contains("|".join(_SERIOUS_KEYWORDS))
            ).then(pl.lit(2, dtype=pl.Int8))
            .when(
                combined.str.contains("|".join(_MINOR_KEYWORDS))
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
        )

        # Status mapping based on statuscurrent
        # "Under Investigation", "Initiated", "NOV Issued", "Warning",
        # "Stop Work Issued", etc. = open
        # "Completed", "Closed", "Compliance Achieved", "Withdrawn" = closed
        closed_statuses = [
            "completed", "closed", "compliance achieved", "withdrawn",
            "open duplicate",
        ]
        status_expr = (
            pl.when(
                pl.col("statuscurrent").str.to_lowercase().is_in(closed_statuses)
            ).then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("recordnum").cast(pl.Utf8).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("opendate").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("seattle").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties (synthesized from complaints — one row per address)
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "seattle_complaints.parquet")

        bbl_expr = (
            pl.lit("sea-")
            + pl.col("originaladdress1").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                bbl_expr.alias("registration_id"),
                pl.col("originaladdress1").alias("address"),
            )
            .unique(subset=["bbl"])
            .with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
                pl.lit("seattle").alias("jurisdiction"),
            )
        )

        return props.select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts (empty — no owner data in Seattle open data)
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
