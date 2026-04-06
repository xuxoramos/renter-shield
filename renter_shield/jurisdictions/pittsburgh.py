"""Pittsburgh jurisdiction adapter — PLI/DOMI/ES Violations + Allegheny County Assessments.

Data sources (CKAN on data.wprdc.org):
  - Pittsburgh PLI/DOMI/ES Violations Report: resource_id
    ``70c06278-92c5-4040-ab28-17671866f81c`` — ~565K rows.  Covers building
    maintenance, fire safety, vacant buildings, refuse, weeds/debris, and
    permits.
  - Allegheny County Property Assessments: resource_id
    ``9a1c60bd-f9f7-4aba-aeb7-af8c3aaa44e5`` — ~585K parcels (county-wide;
    filtered to Pittsburgh at load time via MUNICODE).  Provides building
    characteristics (year built, style, rooms) but no owner name field.
  - PLI Permits: resource_id
    ``f4d1177a-f597-4c32-8cbf-7885f56253f6`` — ~61K building permits with
    ``owner_name`` and ``parcel_num``.  Owner names come from permit
    applications — the property owner on record when the permit was filed.
    Provides the contact/ownership link for scoring.

``parcel_id`` from the violations dataset uses the same format as ``PARID``
in the assessments dataset and ``parcel_num`` in the permits dataset
(e.g. ``0126C00219000000``), enabling a direct join across all three.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# CKAN datastore API
_CKAN_BASE = "https://data.wprdc.org/api/3/action/datastore_search"

# Resource IDs
_VIOLATIONS_ID = "70c06278-92c5-4040-ab28-17671866f81c"
_ASSESSMENT_ID = "9a1c60bd-f9f7-4aba-aeb7-af8c3aaa44e5"
_PERMITS_ID = "f4d1177a-f597-4c32-8cbf-7885f56253f6"

# ---------------------------------------------------------------------------
# Severity keywords — mapped from case_file_type + violation_description
# Sorted alphabetically within each tier; rationale follows each group.
# ---------------------------------------------------------------------------

# Tier 1: Immediately hazardous — fire, structural collapse, vacant/condemned
_CRITICAL_KEYWORDS = [
    "collapse",
    "condemned",
    "dangerous",
    "emergency",
    "fire safety",
    "hazard",
    "imminent",
    "structural",
    "unsafe",
    "vacant building",
]

# Tier 2: Serious habitability — building envelope, plumbing, electrical
_SERIOUS_KEYWORDS = [
    "building maintenance",
    "building without permit",
    "electrical",
    "elevator",
    "exterior",
    "interior",
    "nuisance",
    "plumbing",
    "zoning",
]

# Tier 3: Minor / quality-of-life — debris, refuse, weeds, signs
_MINOR_KEYWORDS = [
    "debris",
    "graffiti",
    "litter",
    "refuse",
    "sign",
    "trash",
    "weeds",
]

# Statuses that indicate a closed case
_CLOSED_STATUSES = [
    "closed",
    "compliance",
]


def _download_ckan_resource(
    resource_id: str, out_path: Path, limit: int = 1_000_000,
) -> int:
    """Download all records from a CKAN datastore resource in batches."""
    all_records: list[dict] = []
    offset = 0
    batch_size = 32_000  # CKAN default max is 32K per request

    while offset < limit:
        query_params: dict = {
            "resource_id": resource_id,
            "limit": min(batch_size, limit - offset),
            "offset": offset,
        }
        params = urllib.parse.urlencode(query_params)
        url = f"{_CKAN_BASE}?{params}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "renter-shield/0.1")
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())

        records = data["result"]["records"]
        if not records:
            break
        all_records.extend(records)
        offset += len(records)
        if offset % 100_000 < batch_size:
            print(f"  … {offset:,} rows fetched")

    df = pl.DataFrame(all_records, infer_schema_length=None)
    df.write_parquet(out_path, compression="zstd", compression_level=3)
    return len(df)


class PittsburghAdapter(JurisdictionAdapter):
    jurisdiction_code = "pittsburgh"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        print("[pittsburgh] downloading PLI/DOMI/ES violations (paginated)…")
        n = _download_ckan_resource(
            _VIOLATIONS_ID,
            self.data_dir / "pittsburgh_violations.parquet",
        )
        print(f"[pittsburgh] saved {n} violation rows")

        print("[pittsburgh] downloading Allegheny County assessments (paginated)…")
        n = _download_ckan_resource(
            _ASSESSMENT_ID,
            self.data_dir / "pittsburgh_assessment.parquet",
        )
        print(f"[pittsburgh] saved {n} assessment rows")

        print("[pittsburgh] downloading PLI permits (paginated)…")
        n = _download_ckan_resource(
            _PERMITS_ID,
            self.data_dir / "pittsburgh_permits.parquet",
        )
        print(f"[pittsburgh] saved {n} permit rows")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        path = self.data_dir / "pittsburgh_violations.parquet"
        if not path.exists():
            raise FileNotFoundError(
                "No Pittsburgh violation data found. Run download() first."
            )

        raw = pl.scan_parquet(path)

        # Filter to analysis window
        raw = raw.filter(
            pl.col("investigation_date").is_not_null()
            & (pl.col("investigation_date").cast(pl.Utf8) >= MIN_DATE)
        )

        # Violation ID: casefile_number (unique per violation event)
        vid_expr = pl.col("casefile_number").cast(pl.Utf8)

        # BBL: "pit-" + parcel_id
        bbl_expr = (
            pl.lit("pit-")
            + pl.col("parcel_id").cast(pl.Utf8).fill_null("unknown")
        )

        # Severity by keyword matching on case_file_type + violation_description
        text_lower = (
            pl.col("case_file_type").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("violation_description").cast(pl.Utf8).fill_null("")
        ).str.to_lowercase()

        severity_expr = (
            pl.when(
                text_lower.str.contains("|".join(_CRITICAL_KEYWORDS))
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                text_lower.str.contains("|".join(_SERIOUS_KEYWORDS))
            ).then(pl.lit(2, dtype=pl.Int8))
            .when(
                text_lower.str.contains("|".join(_MINOR_KEYWORDS))
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))  # default to serious
        )

        # Status: check against closed statuses
        status_lower = pl.col("status").cast(pl.Utf8).str.to_lowercase().fill_null("")
        status_expr = (
            pl.when(
                status_lower.str.contains("|".join(_CLOSED_STATUSES))
            ).then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            vid_expr.alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("investigation_date").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("pittsburgh").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        viol_path = self.data_dir / "pittsburgh_violations.parquet"
        if not viol_path.exists():
            raise FileNotFoundError(
                "No Pittsburgh violation data found. Run download() first."
            )

        raw = pl.scan_parquet(viol_path)

        bbl_expr = (
            pl.lit("pit-")
            + pl.col("parcel_id").cast(pl.Utf8).fill_null("unknown")
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                pl.col("parcel_id").cast(pl.Utf8).alias("registration_id"),
                pl.col("address").cast(pl.Utf8).alias("address"),
            )
            .unique(subset=["bbl"])
        )

        # Enrich from Allegheny County assessment data if downloaded
        assess_path = self.data_dir / "pittsburgh_assessment.parquet"
        if assess_path.exists():
            assess = pl.scan_parquet(assess_path)
            # Filter to City of Pittsburgh (MUNICODE 101-132 are Pittsburgh wards)
            assess = (
                assess.filter(
                    pl.col("MUNICODE").cast(pl.Int64, strict=False).is_between(101, 132)
                )
                .select(
                    (pl.lit("pit-") + pl.col("PARID").cast(pl.Utf8)).alias("bbl"),
                    pl.col("YEARBLT").cast(pl.Utf8).alias("year_built"),
                    pl.col("TOTALROOMS").cast(pl.Float64, strict=False).alias("units_residential"),
                )
                .unique(subset=["bbl"])
            )
            props = props.join(assess, on="bbl", how="left")
        else:
            props = props.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            )

        return props.with_columns(
            pl.lit("pittsburgh").alias("jurisdiction"),
        ).select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts — owner names from PLI Permits
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        permits_path = self.data_dir / "pittsburgh_permits.parquet"
        if not permits_path.exists():
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

        raw = pl.scan_parquet(permits_path)

        # Filter to rows with both owner_name and parcel_num
        raw = raw.filter(
            pl.col("owner_name").is_not_null()
            & (pl.col("owner_name").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("parcel_num").is_not_null()
            & (pl.col("parcel_num").cast(pl.Utf8).str.strip_chars() != "")
        )

        # owner_name may be a person ("JOHN SMITH") or entity ("FP ISABELLA LLC").
        # Heuristic: if it contains LLC/LP/INC/CORP/TRUST/ASSOC/PARTNERS or
        # has no space, treat as business_name; otherwise split first/last.
        name_col = pl.col("owner_name").cast(pl.Utf8).str.strip_chars()
        is_entity = name_col.str.to_uppercase().str.contains(
            r"(?i)\b(LLC|LP|INC|CORP|TRUST|ASSOC|PARTNERS|PARTNERSHIP|LTD|L\.P\.|AUTHORITY|HOUSING|BANK|CHURCH|CITY OF|COUNTY OF|COMMONWEALTH|UNIVERSITY|SCHOOL|HOSPITAL|FOUNDATION|ESTATE)\b"
        )

        # For persons: first token = first name, rest = last name
        # For entities: full string goes to business_name
        contacts = raw.select(
            pl.col("parcel_num").cast(pl.Utf8).str.strip_chars().alias("registration_id"),
            pl.when(is_entity)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(
                name_col.str.split(" ").list.first()
            ).alias("first_name"),
            pl.when(is_entity)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(
                name_col.str.replace(r"^\S+\s*", "")
            ).alias("last_name"),
            pl.when(is_entity)
            .then(name_col)
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("business_name"),
            pl.lit(None, dtype=pl.Utf8).alias("business_house_number"),
            pl.col("address").cast(pl.Utf8).alias("business_street"),
            pl.lit("pittsburgh").alias("jurisdiction"),
        )

        # Deduplicate: one owner per parcel (take first permit record)
        return contacts.unique(subset=["registration_id", "first_name", "last_name", "business_name"])
