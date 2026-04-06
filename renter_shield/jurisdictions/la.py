"""Los Angeles jurisdiction adapter — LADBS Code Enforcement Cases.

Data sources (Socrata, data.lacity.org):
  - Open cases  (u82d-eh7z): ~29K active enforcement cases
  - Closed cases (rken-a55j): ~809K resolved enforcement cases

Each row is a **case**, not an individual violation.  Fields available:
  apno (case number), apname (inspection district), stno/stsub/predir/
  stname/suffix/postdir (address components), zip, adddttm (date opened),
  resdttm (date closed), prclid (Parcel Identification Number / PIN),
  aptype (case type), apc (Area Planning Commission), stat (O or C).

Limitations:
  - **No violation descriptions** — only case type (GENERAL, PACE, etc.).
    Severity mapping is coarser than cities with free-text descriptions.
  - **No owner data** — LADBS does not publish owner/contact information.
    Properties are keyed by Parcel ID (PIN).  LA will contribute
    address-searchable violation + property data but no scored landlords
    unless an owner dataset is added later.

Case types and severity mapping rationale:
  - CITATIONS: formal administrative citation = confirmed violation with
    penalty → Tier 2 (Serious)
  - CNAP: Community Nuisance Abatement Program = habitability/nuisance
    → Tier 2 (Serious)
  - GENERAL: general complaint investigations → Tier 3 (Minor)
  - PACE: Pro-Active Code Enforcement sweeps → Tier 3 (Minor)
  - NAR: Neighborhood Assessment Review → Tier 3 (Minor)
  - VEIP: Vacant/Existing Improvement Program → Tier 3 (Minor)
  - BILLBOARDS, SIGNS, CARTS: non-housing → excluded
  - FRP, LEA, IHTF, XXX, CNA: unknown/rare → Tier 3 (Minor) default
"""

from __future__ import annotations

import time

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# Socrata dataset identifiers on data.lacity.org
_OPEN_CASES_ID = "u82d-eh7z"
_CLOSED_CASES_ID = "rken-a55j"

# Non-housing case types — excluded entirely
_EXCLUDED_CASE_TYPES = {"BILLBOARDS", "SIGNS", "CARTS"}

# Pagination / retry settings
_SOCRATA_PAGE_SIZE = 50_000
_SOCRATA_TIMEOUT = 120
_SOCRATA_RETRIES = 3

# Case type → severity tier mapping
# Tier 2 = confirmed enforcement action or habitability issue
# Tier 3 = general investigations/inspections (severity unknown without
#           description text — conservative default)
_TIER2_CASE_TYPES = ["citations", "cnap"]
_TIER3_CASE_TYPES = ["general", "pace", "nar", "veip", "frp", "lea", "ihtf", "xxx", "cna"]


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


class LAAdapter(JurisdictionAdapter):
    jurisdiction_code = "la"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        self.data_dir.mkdir(parents=True, exist_ok=True)
        client = Socrata("data.lacity.org", None)

        where = (
            f"adddttm >= '{MIN_DATE}' AND "
            f"aptype NOT IN ('BILLBOARDS', 'SIGNS', 'CARTS')"
        )

        # Open cases
        print("[la] downloading open enforcement cases (paginated)…")
        open_rows = _paginated_get(client, _OPEN_CASES_ID, where=where)
        print(f"[la] open cases: {len(open_rows)} rows")

        # Closed cases
        print("[la] downloading closed enforcement cases (paginated)…")
        closed_rows = _paginated_get(client, _CLOSED_CASES_ID, where=where)
        print(f"[la] closed cases: {len(closed_rows)} rows")

        # Combine and save
        all_rows = open_rows + closed_rows
        if not all_rows:
            print("[la] WARNING: no rows returned from either dataset")
            return

        df = pl.DataFrame(all_rows)
        out = self.data_dir / "la_cases.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[la] saved {len(df)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "la_cases.parquet")

        raw = raw.filter(pl.col("adddttm").is_not_null())

        # Filter out non-housing case types (belt-and-suspenders — also
        # filtered during download, but data files might be from older runs)
        type_upper = pl.col("aptype").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        raw = raw.filter(~type_upper.is_in(list(_EXCLUDED_CASE_TYPES)))

        # BBL from Parcel ID: strip all whitespace, prefix with "la-"
        bbl_expr = (
            pl.lit("la-")
            + pl.col("prclid").cast(pl.Utf8).str.replace_all(r"\s+", "")
        )

        # Severity mapping by case type
        type_lower = pl.col("aptype").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        severity_expr = (
            pl.when(type_lower.is_in(_TIER2_CASE_TYPES))
            .then(pl.lit(2, dtype=pl.Int8))
            .otherwise(pl.lit(3, dtype=pl.Int8))  # default Tier 3 for unknowns
        )

        # Status: use the stat column directly (O = open, C = closed)
        status_expr = (
            pl.when(pl.col("stat").cast(pl.Utf8).str.to_uppercase() == "C")
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("apno").cast(pl.Utf8).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("adddttm").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("la").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "la_cases.parquet")

        # BBL from Parcel ID
        bbl_expr = (
            pl.lit("la-")
            + pl.col("prclid").cast(pl.Utf8).str.replace_all(r"\s+", "")
        )

        # Build address from components:
        # {stno} {stsub} {predir} {stname} {suffix} {postdir}, ZIP
        # Some columns may be absent when Socrata drops all-null fields
        available = set(raw.collect_schema().names())
        addr_parts = [
            pl.col("stno").cast(pl.Utf8).fill_null(""),
        ]
        if "stsub" in available:
            addr_parts.append(pl.col("stsub").cast(pl.Utf8).fill_null(""))
        addr_parts.append(pl.col("predir").cast(pl.Utf8).fill_null(""))
        addr_parts.append(pl.col("stname").cast(pl.Utf8).fill_null(""))
        addr_parts.append(pl.col("suffix").cast(pl.Utf8).fill_null(""))
        if "postdir" in available:
            addr_parts.append(pl.col("postdir").cast(pl.Utf8).fill_null(""))
        # Concatenate with spaces, then collapse multiple spaces
        address_expr = (
            pl.concat_str(addr_parts, separator=" ")
            .str.replace_all(r"\s{2,}", " ")
            .str.strip_chars()
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                bbl_expr.alias("registration_id"),
                address_expr.alias("address"),
                pl.col("zip").cast(pl.Utf8).str.strip_chars().alias("zip"),
            )
            .unique(subset=["bbl"])
            .with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
                pl.lit("la").alias("jurisdiction"),
            )
        )

        return props.select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts (empty — no owner data in LADBS data)
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
