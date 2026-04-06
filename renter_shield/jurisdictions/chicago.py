"""Chicago jurisdiction adapter — Building Violations + Scofflaw List.

Data sources:
  - Building Violations (22u3-xenr): ~2M violations from 2006-present.
    Has: violation_code, violation_status, violation_date, address,
    property_group, department_bureau, violation_description.
    Does NOT have: owner name.
  - Building Code Scofflaw List (crg5-4zyp): ~659 records of the worst
    offending building owners. Has: defendant_owner, address.

Chicago does not publish a general property-owner registry like NYC's HPD
registrations.  Ownership data comes only from the Scofflaw list and from
any future Cook County Assessor integration.  The adapter synthesises a
contacts table from the Scofflaw defendant_owner field so the ownership
network resolver can group them.
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
                           page_size: int = _SOCRATA_PAGE_SIZE) -> list[dict]:
    """Fetch all rows from a Socrata dataset using offset pagination."""
    import time
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


# Socrata dataset identifiers
_VIOLATIONS_ID = "22u3-xenr"
_SCOFFLAW_ID = "crg5-4zyp"

# Chicago doesn't have A/B/C classes. We map severity by department_bureau
# and keyword heuristics on violation_description.
_CRITICAL_BUREAUS = {"ELEVATOR", "BOILER", "REFRIGERATION"}
_CRITICAL_KEYWORDS = [
    "lead", "fire", "smoke detector", "carbon monoxide", "structural",
    "hazard", "unsafe", "collapse", "imminent", "emergency",
    "no heat", "no hot water", "vermin", "rat", "roach", "bedbug",
]
_MINOR_KEYWORDS = [
    "registration", "permit", "plans", "sign", "posted", "file",
    "arrange", "inspection",
]


def _classify_severity(description: str | None, bureau: str | None) -> int:
    """Map a Chicago violation to a universal severity tier (1-4)."""
    desc = (description or "").lower()
    bur = (bureau or "").upper()

    if bur in _CRITICAL_BUREAUS:
        return 1
    if any(kw in desc for kw in _CRITICAL_KEYWORDS):
        return 1
    if any(kw in desc for kw in _MINOR_KEYWORDS):
        return 3
    # Default: most Conservation / code enforcement = Tier 2 (Serious)
    return 2


class ChicagoAdapter(JurisdictionAdapter):
    jurisdiction_code = "chicago"

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        client = Socrata("data.cityofchicago.org", None)

        # Violations — paginated to get all records
        print("[chicago] downloading violations (paginated)…")
        rows = _paginated_socrata_get(
            client, _VIOLATIONS_ID,
            where=f"violation_date >= '{MIN_DATE}'",
        )
        df = pl.DataFrame(rows)
        out = self.data_dir / "chicago_violations.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[chicago] saved {len(df)} violation rows → {out}")

        # Scofflaw list (small, ~659 rows)
        print("[chicago] downloading scofflaw list…")
        rows = client.get(_SCOFFLAW_ID, limit=5_000)
        df = pl.DataFrame(rows)
        out = self.data_dir / "chicago_scofflaw.parquet"
        df.write_parquet(out, compression="zstd", compression_level=3)
        print(f"[chicago] saved {len(df)} scofflaw rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "chicago_violations.parquet")

        raw = raw.filter(pl.col("violation_date") >= MIN_DATE)

        # Chicago uses property_group as lot identifier (similar to BBL)
        # Prefix with "chi-" to namespace
        bbl_expr = pl.lit("chi-") + pl.col("property_group").cast(pl.Utf8)

        # Severity: we need to collect for Python-side classification,
        # so we do it via a struct + map approach.  For lazy compat, use
        # when/then chains on department_bureau + keyword heuristics.
        severity_expr = (
            pl.when(
                pl.col("department_bureau").is_in(list(_CRITICAL_BUREAUS))
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                pl.col("violation_description").str.to_lowercase().str.contains(
                    "|".join(_CRITICAL_KEYWORDS)
                )
            ).then(pl.lit(1, dtype=pl.Int8))
            .when(
                pl.col("violation_description").str.to_lowercase().str.contains(
                    "|".join(_MINOR_KEYWORDS)
                )
            ).then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(pl.col("violation_status") == "COMPLIED")
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("id").cast(pl.Utf8).alias("violation_id"),
            bbl_expr.alias("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("violation_date").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("chicago").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties  (synthesized from violations — unique property_groups)
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "chicago_violations.parquet")

        # One row per property_group with address info
        # Chicago doesn't publish unit counts; default to 1
        props = (
            raw.select(
                (pl.lit("chi-") + pl.col("property_group").cast(pl.Utf8)).alias("bbl"),
                pl.col("property_group").cast(pl.Utf8).alias("registration_id"),
                pl.col("address"),
            )
            .unique(subset=["bbl"])
            .with_columns(
                pl.lit(1.0).alias("units_residential"),
                pl.lit(None, dtype=pl.Utf8).alias("year_built"),
                pl.lit("chicago").alias("jurisdiction"),
            )
        )
        return props

    # ------------------------------------------------------------------
    # load_contacts  (from Scofflaw list — defendant_owner + address)
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        path = self.data_dir / "chicago_scofflaw.parquet"
        if not path.exists():
            # Return empty frame with correct schema
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

        raw = pl.scan_parquet(path)

        # Build address → property_group lookup from violations data
        viol_path = self.data_dir / "chicago_violations.parquet"
        if not viol_path.exists():
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

        viols = pl.scan_parquet(viol_path)
        addr_lookup = (
            viols.select(
                pl.col("address").str.to_uppercase().str.strip_chars().alias("addr_upper"),
                pl.col("property_group").cast(pl.Utf8).alias("registration_id"),
            )
            .unique(subset=["addr_upper"])
        )

        # defendant_owner is a single string; split into first/last heuristically
        # address is the building address (used as business address for grouping)
        contacts = raw.select(
            pl.col("address").str.to_uppercase().str.strip_chars().alias("addr_upper"),
            pl.col("address").alias("_addr"),
            pl.col("defendant_owner").str.to_uppercase().str.strip_chars().alias("_owner"),
        ).with_columns(
            # Heuristic: if owner contains comma, assume "LAST, FIRST"
            # Otherwise take first token as first name, rest as last name
            pl.when(pl.col("_owner").str.contains(","))
            .then(pl.col("_owner").str.split(",").list.get(1, null_on_oob=True).str.strip_chars())
            .otherwise(pl.col("_owner").str.split(" ").list.get(0, null_on_oob=True))
            .alias("first_name"),
            pl.when(pl.col("_owner").str.contains(","))
            .then(pl.col("_owner").str.split(",").list.get(0, null_on_oob=True).str.strip_chars())
            .otherwise(
                pl.col("_owner").str.replace(r"^\S+\s*", "")
            )
            .alias("last_name"),
        )

        # Bridge scofflaw address → property_group (= properties.registration_id)
        return (
            contacts.join(addr_lookup, on="addr_upper", how="inner")
            .select(
                "registration_id",
                pl.col("first_name"),
                pl.col("last_name"),
                pl.col("_owner").alias("business_name"),
                pl.lit(None, dtype=pl.Utf8).alias("business_house_number"),
                pl.col("_addr").alias("business_street"),
                pl.lit("chicago").alias("jurisdiction"),
            )
        )
