"""NYC jurisdiction adapter — HPD violations, registrations, contacts, and PLUTO."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from renter_shield.config import MIN_DATE
from renter_shield.jurisdictions.base import JurisdictionAdapter

# HPD violation class → universal severity tier
_NYC_SEVERITY_MAP = {
    "C": 1,   # Immediately hazardous  → Tier 1 (Critical)
    "B": 2,   # Hazardous              → Tier 2 (Serious)
    "A": 3,   # Non-hazardous          → Tier 3 (Minor)
    "I": 4,   # Informational          → Tier 4
}

# Socrata page size — fetch this many records per request, then offset
_SOCRATA_PAGE_SIZE = 50_000
_SOCRATA_TIMEOUT = 120  # seconds per request
_SOCRATA_RETRIES = 3


def _paginated_get(client, dataset_id: str, *, where: str | None = None,
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


def _make_bbl(df: pl.LazyFrame, boro: str, block: str, lot: str) -> pl.LazyFrame:
    """Append a 10-digit BBL column: Borough(1) + Block(5) + Lot(4)."""
    return df.with_columns(
        (
            pl.col(boro).cast(pl.Utf8).str.zfill(1)
            + pl.col(block).cast(pl.Utf8).str.zfill(5)
            + pl.col(lot).cast(pl.Utf8).str.zfill(4)
        ).alias("bbl")
    )


class NYCAdapter(JurisdictionAdapter):
    jurisdiction_code = "nyc"

    def __init__(self, data_dir: Path) -> None:
        super().__init__(data_dir)
        self._pluto_path = data_dir / "pluto_25v3_1.parquet"

    # ------------------------------------------------------------------
    # download  (optional — uses sodapy)
    # ------------------------------------------------------------------
    def download(self) -> None:
        try:
            from sodapy import Socrata  # noqa: WPS433
        except ImportError as exc:
            raise ImportError("pip install sodapy to use automatic download") from exc

        client = Socrata("data.cityofnewyork.us", None)

        datasets = {
            "hpd_violations": {
                "id": "wvxf-dwi5",
                "where": f"InspectionDate >= '{MIN_DATE}'",
            },
            "hpd_registrations": {
                "id": "tesw-yqqr",
                "where": None,
            },
            "hpd_contacts": {
                "id": "feu5-w2e2",
                "where": None,
            },
        }

        for name, cfg in datasets.items():
            print(f"[nyc] downloading {name} (paginated)…")
            rows = _paginated_get(client, cfg["id"], where=cfg.get("where"))
            df = pl.DataFrame(rows)
            out = self.data_dir / f"{name}.parquet"
            df.write_parquet(out, compression="zstd", compression_level=3)
            print(f"[nyc] saved {len(df)} rows → {out}")

    # ------------------------------------------------------------------
    # load_violations
    # ------------------------------------------------------------------
    def load_violations(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "hpd_violations.parquet")

        raw = raw.filter(pl.col("inspectiondate") >= MIN_DATE)

        raw = _make_bbl(raw, "boroid", "block", "lot")

        # Map NYC class → severity tier
        severity_expr = (
            pl.when(pl.col("class") == "C").then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("class") == "B").then(pl.lit(2, dtype=pl.Int8))
            .when(pl.col("class") == "A").then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(4, dtype=pl.Int8))
        )

        status_expr = (
            pl.when(pl.col("violationstatus") == "Close")
            .then(pl.lit("closed"))
            .otherwise(pl.lit("open"))
        )

        return raw.select(
            pl.col("violationid").cast(pl.Utf8).alias("violation_id"),
            pl.col("bbl"),
            severity_expr.alias("severity_tier"),
            status_expr.alias("status"),
            pl.col("inspectiondate").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("inspection_date"),
            pl.lit("nyc").alias("jurisdiction"),
        )

    # ------------------------------------------------------------------
    # load_properties  (registrations + optional PLUTO enrichment)
    # ------------------------------------------------------------------
    def load_properties(self) -> pl.LazyFrame:
        regs = pl.scan_parquet(self.data_dir / "hpd_registrations.parquet")
        regs = _make_bbl(regs, "boroid", "block", "lot")

        props = regs.select(
            pl.col("bbl"),
            pl.col("registrationid").cast(pl.Utf8).alias("registration_id"),
            pl.lit(None, dtype=pl.Float64).alias("units_residential"),
            pl.lit(None, dtype=pl.Utf8).alias("year_built"),
            pl.lit(None, dtype=pl.Utf8).alias("address"),
            pl.lit("nyc").alias("jurisdiction"),
        )

        # Build addresses from HPD violations (always available)
        viol_path = self.data_dir / "hpd_violations.parquet"
        if viol_path.exists():
            viols = pl.scan_parquet(viol_path)
            viols = _make_bbl(viols, "boroid", "block", "lot")
            viol_addrs = (
                viols.select(
                    pl.col("bbl"),
                    (
                        pl.col("housenumber").cast(pl.Utf8).fill_null("")
                        + pl.lit(" ")
                        + pl.col("streetname").cast(pl.Utf8).fill_null("")
                        + pl.lit(", ")
                        + pl.col("boro").cast(pl.Utf8).fill_null("")
                    ).str.strip_chars().alias("viol_address"),
                )
                .filter(pl.col("viol_address").str.strip_chars() != ",")
                .unique(subset=["bbl"])
            )
            props = props.join(viol_addrs, on="bbl", how="left").with_columns(
                pl.coalesce(["address", "viol_address"]).alias("address"),
            ).select(
                "bbl", "registration_id", "units_residential",
                "year_built", "address", "jurisdiction",
            )

        # Enrich with PLUTO if available
        if self._pluto_path.exists():
            pluto = pl.scan_parquet(self._pluto_path)
            pluto = _make_bbl(pluto, "borough", "block", "lot")
            pluto = pluto.select(
                pl.col("bbl"),
                pl.col("unitsres").cast(pl.Float64).alias("pluto_units"),
                pl.col("yearbuilt").cast(pl.Utf8).alias("pluto_year"),
                pl.col("address").alias("pluto_address"),
            )
            props = props.join(pluto, on="bbl", how="left", suffix="_pluto").with_columns(
                pl.coalesce(["units_residential", "pluto_units"]).alias("units_residential"),
                pl.coalesce(["year_built", "pluto_year"]).alias("year_built"),
                pl.coalesce(["address", "pluto_address"]).alias("address"),
            ).select(
                "bbl", "registration_id", "units_residential",
                "year_built", "address", "jurisdiction",
            )

        return props

    # ------------------------------------------------------------------
    # load_contacts
    # ------------------------------------------------------------------
    def load_contacts(self) -> pl.LazyFrame:
        raw = pl.scan_parquet(self.data_dir / "hpd_contacts.parquet")

        return raw.select(
            pl.col("registrationid").cast(pl.Utf8).alias("registration_id"),
            pl.col("firstname").str.to_uppercase().str.strip_chars().alias("first_name"),
            pl.col("lastname").str.to_uppercase().str.strip_chars().alias("last_name"),
            pl.col("corporationname").str.to_uppercase().str.strip_chars().alias("business_name"),
            pl.col("businesshousenumber").cast(pl.Utf8).alias("business_house_number"),
            pl.col("businessstreetname").cast(pl.Utf8).alias("business_street"),
            pl.lit("nyc").alias("jurisdiction"),
        )
