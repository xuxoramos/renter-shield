"""Boston jurisdiction adapter — Code Enforcement + Building & Property Violations.

Data sources (CKAN on data.boston.gov):
  - Code Enforcement Violations (Public Works): resource_id
    ``90ed3816-5e70-443c-803d-9a71f44470be`` — ~885K rows.  Covers trash,
    sanitation, and housing code complaints.
  - Building & Property Violations (ISD): resource_id
    ``800a2663-1d6a-46e7-9356-bedb70f5332c`` — ~17K rows.  Covers unsafe
    structures, fire code, and building code issues.
  - Property Assessment FY2026 (Assessing Department): resource_id
    ``ee73430d-96c0-423e-ad21-c4cfb54c8961`` — ~184K parcels.  Provides
    owner name (OWNER), mailing address (MAIL_*), year built (YR_BUILT),
    and land use.  Linked by PID matching sam_id from violation datasets.

Both violation datasets share the same column structure.  They are downloaded
separately and unioned at load time.  ``sam_id`` (the parcel identifier in
the Boston Assessing database) is used as the property key.
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
_CKAN_BASE = "https://data.boston.gov/api/3/action/datastore_search"

# Resource IDs
_CODE_ENFORCEMENT_ID = "90ed3816-5e70-443c-803d-9a71f44470be"
_BUILDING_VIOLATIONS_ID = "800a2663-1d6a-46e7-9356-bedb70f5332c"
_ASSESSMENT_FY2026_ID = "ee73430d-96c0-423e-ad21-c4cfb54c8961"

# Description keywords → severity tier
_CRITICAL_KEYWORDS = [
    "unsafe", "dangerous", "collapse", "imminent", "hazard", "emergency",
    "fire", "structural", "lead", "smoke detector", "carbon monoxide",
    "no heat", "no hot water",
]
_SERIOUS_KEYWORDS = [
    "plumbing", "electrical", "elevator", "boiler", "egress",
    "handrail", "stairway", "means of egress",
]
_MINOR_KEYWORDS = [
    "trash", "rubbish", "litter", "sign", "permit", "registration",
    "overgrown", "fence", "graffiti", "storage",
]


def _download_ckan_resource(
    resource_id: str, out_path: Path, limit: int = 500_000,
    filters: dict | None = None,
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
        if filters:
            query_params["filters"] = json.dumps(filters)
        params = urllib.parse.urlencode(query_params)
        url = f"{_CKAN_BASE}?{params}"
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())

        records = data["result"]["records"]
        if not records:
            break
        all_records.extend(records)
        offset += len(records)

    df = pl.DataFrame(all_records)
    df.write_parquet(out_path, compression="zstd", compression_level=3)
    return len(df)


class BostonAdapter(JurisdictionAdapter):
    jurisdiction_code = "boston"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

        print("[boston] downloading code enforcement violations (paginated)…")
        n = _download_ckan_resource(
            _CODE_ENFORCEMENT_ID,
            self.data_dir / "boston_code_enforcement.parquet",
            limit=2_000_000,
        )
        print(f"[boston] saved {n} code enforcement rows")

        print("[boston] downloading building & property violations…")
        n = _download_ckan_resource(
            _BUILDING_VIOLATIONS_ID,
            self.data_dir / "boston_building_violations.parquet",
        )
        print(f"[boston] saved {n} building violation rows")

        print("[boston] downloading property assessment (FY2026)…")
        n = _download_ckan_resource(
            _ASSESSMENT_FY2026_ID,
            self.data_dir / "boston_assessment.parquet",
            limit=200_000,
        )
        print(f"[boston] saved {n} assessment rows")

    # ------------------------------------------------------------------
    # _load_raw — union both datasets
    # ------------------------------------------------------------------
    def _load_raw(self) -> pl.LazyFrame:
        frames: list[pl.LazyFrame] = []

        # Shared columns across both datasets
        keep_cols = [
            "case_no", "status_dttm", "status", "code", "value",
            "description", "violation_stno", "violation_sthigh",
            "violation_street", "violation_suffix", "violation_city",
            "violation_zip", "sam_id", "latitude", "longitude",
        ]

        ce_path = self.data_dir / "boston_code_enforcement.parquet"
        if ce_path.exists():
            ce = pl.scan_parquet(ce_path)
            # Code enforcement has ticket_no; building has ap_case_defn_key
            ce = ce.with_columns(
                pl.col("ticket_no").cast(pl.Utf8).alias("sub_id"),
            ).select(keep_cols + ["sub_id"])
            frames.append(ce)

        bv_path = self.data_dir / "boston_building_violations.parquet"
        if bv_path.exists():
            bv = pl.scan_parquet(bv_path)
            bv = bv.with_columns(
                pl.col("ap_case_defn_key").cast(pl.Utf8).alias("sub_id"),
            ).select(keep_cols + ["sub_id"])
            frames.append(bv)

        if not frames:
            raise FileNotFoundError(
                "No Boston data files found. Run download() first."
            )

        return pl.concat(frames, how="vertical_relaxed")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = self._load_raw()

        raw = raw.filter(
            pl.col("status_dttm").is_not_null()
            & (pl.col("status_dttm") >= MIN_DATE)
        )

        # Violation ID: case_no + "_" + sub_id
        vid_expr = (
            pl.col("case_no").cast(pl.Utf8)
            + pl.lit("_")
            + pl.col("sub_id").cast(pl.Utf8)
        )

        # BBL: "bos-" + sam_id
        bbl_expr = pl.lit("bos-") + pl.col("sam_id").cast(pl.Utf8)

        # Severity by description keyword matching
        desc_lower = pl.col("description").str.to_lowercase().fill_null("")
        severity_expr = (
            pl.when(
                desc_lower.str.contains("|".join(_CRITICAL_KEYWORDS))
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                desc_lower.str.contains("|".join(_SERIOUS_KEYWORDS))
            ).then(pl.lit(2, dtype=pl.Int8))
            .when(
                desc_lower.str.contains("|".join(_MINOR_KEYWORDS))
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(pl.col("status").str.to_lowercase() == "open")
            .then(pl.lit("open"))
            .otherwise(pl.lit("closed"))
        )

        return raw.select(
            vid_expr.alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("status_dttm").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("boston").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # _sam_to_pid_lookup — bridge violation sam_id → assessment PID via
    # address matching (the two datasets share no common numeric key).
    # ------------------------------------------------------------------
    def _sam_to_pid_lookup(self) -> pl.LazyFrame | None:
        """Return a lazy sam_id → PID mapping, or None if no assessment data."""
        assessment_path = self.data_dir / "boston_assessment.parquet"
        if not assessment_path.exists():
            return None

        raw = self._load_raw()
        assess = pl.scan_parquet(assessment_path)

        # Build normalised address keys on both sides.
        # Violations: "125 BARTLETT ST"  Assessment: "125 Bartlett"
        viol_addrs = (
            raw.select(
                pl.col("sam_id").cast(pl.Utf8).alias("sam_id"),
                (
                    pl.col("violation_stno").cast(pl.Utf8).fill_null("")
                    + pl.lit(" ")
                    + pl.col("violation_street").cast(pl.Utf8).fill_null("")
                    + pl.lit(" ")
                    + pl.col("violation_suffix").cast(pl.Utf8).fill_null("")
                ).str.to_uppercase().str.strip_chars().alias("addr_key"),
            )
            .unique(subset=["sam_id"])
        )

        assess_addrs = (
            assess.select(
                pl.col("PID").cast(pl.Utf8).alias("PID"),
                (
                    pl.col("ST_NUM").cast(pl.Utf8).fill_null("")
                    + pl.lit(" ")
                    + pl.col("ST_NAME").cast(pl.Utf8).fill_null("")
                ).str.to_uppercase().str.strip_chars().alias("addr_key"),
            )
            .unique(subset=["PID"])
        )

        return viol_addrs.join(assess_addrs, on="addr_key", how="inner").select(
            "sam_id", "PID"
        ).unique(subset=["sam_id"])  # keep first PID per sam_id to avoid fan-out

    # ------------------------------------------------------------------
    # load_properties (enriched from assessment data when available)
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = self._load_raw()

        bbl_expr = pl.lit("bos-") + pl.col("sam_id").cast(pl.Utf8)

        addr_expr = (
            pl.col("violation_stno").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("violation_street").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("violation_suffix").cast(pl.Utf8).fill_null("")
        )

        props = (
            raw.select(
                bbl_expr.alias("bbl"),
                pl.col("sam_id").cast(pl.Utf8).alias("registration_id"),
                addr_expr.str.strip_chars().alias("address"),
            )
            .unique(subset=["bbl"])
        )

        # Enrich from assessment data via sam_id → PID address bridge
        assessment_path = self.data_dir / "boston_assessment.parquet"
        lookup = self._sam_to_pid_lookup()
        if assessment_path.exists() and lookup is not None:
            assess = pl.scan_parquet(assessment_path)
            assess = (
                assess.select(
                    pl.col("PID").cast(pl.Utf8).alias("PID"),
                    pl.col("YR_BUILT").cast(pl.Utf8).alias("year_built"),
                    # Prefer RES_UNITS when available; fall back to RES_FLOOR
                    pl.coalesce(
                        pl.col("RES_UNITS").cast(pl.Float64, strict=False),
                        pl.col("RES_FLOOR").cast(pl.Float64, strict=False),
                    ).alias("units_residential"),
                )
                .unique(subset=["PID"])
            )
            # sam_id → PID → assessment columns
            bridge = lookup.join(assess, on="PID", how="inner").select(
                pl.col("sam_id").alias("registration_id"),
                "year_built",
                "units_residential",
            )
            props = props.join(bridge, on="registration_id", how="left")
        else:
            props = props.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            )

        return props.with_columns(
            pl.lit("boston").alias("jurisdiction"),
        ).select(
            "bbl", "registration_id", "units_residential",
            "year_built", "address", "jurisdiction",
        )

    # ------------------------------------------------------------------
    # load_contacts (from assessment OWNER + MAIL_* fields)
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        assessment_path = self.data_dir / "boston_assessment.parquet"
        lookup = self._sam_to_pid_lookup()
        if not assessment_path.exists() or lookup is None:
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

        assess = pl.scan_parquet(assessment_path)

        # OWNER is a single field ("SMITH JOHN" or "ACME TRUST LLC").
        # We treat it as business_name since splitting first/last from a
        # single combined field is unreliable (trusts, LLCs, estates, etc.)
        owner_info = (
            assess.select(
                pl.col("PID").cast(pl.Utf8).alias("PID"),
                pl.col("OWNER").cast(pl.Utf8).alias("business_name"),
                pl.col("ST_NUM").cast(pl.Utf8).alias("business_house_number"),
                pl.col("MAIL_STREET_ADDRESS").cast(pl.Utf8).alias("business_street"),
            )
            .filter(pl.col("business_name").is_not_null())
            .unique(subset=["PID"])
        )

        # Bridge PID → sam_id so registration_id matches properties
        return (
            lookup.join(owner_info, on="PID", how="inner")
            .select(
                pl.col("sam_id").alias("registration_id"),
                pl.lit(None, dtype=pl.Utf8).alias("first_name"),
                pl.lit(None, dtype=pl.Utf8).alias("last_name"),
                "business_name",
                "business_house_number",
                "business_street",
                pl.lit("boston").alias("jurisdiction"),
            )
            .unique(subset=["registration_id"])
        )
