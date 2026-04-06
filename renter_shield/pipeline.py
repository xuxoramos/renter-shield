"""Pipeline orchestrator — glues ingest → normalize → resolve → score.

Writes normalized Parquet intermediates to *output_dir* so that the DuckDB
scoring engine can read them directly at query time (no persistent .duckdb).
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl

from renter_shield.config import JURISDICTION_REGISTRY
from renter_shield.jurisdictions.base import JurisdictionAdapter
from renter_shield.models import (
    CONTACTS_SCHEMA,
    PROPERTIES_SCHEMA,
    VIOLATIONS_SCHEMA,
)
from renter_shield.ownership import resolve_ownership_networks
from renter_shield.scoring import compute_harm_scores


def _load_adapter(jurisdiction: str, data_dir: Path) -> JurisdictionAdapter:
    """Dynamically instantiate a jurisdiction adapter from the registry."""
    class_path = JURISDICTION_REGISTRY.get(jurisdiction)
    if class_path is None:
        raise ValueError(
            f"Unknown jurisdiction '{jurisdiction}'. "
            f"Registered: {list(JURISDICTION_REGISTRY)}"
        )
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    adapter_cls = getattr(module, class_name)
    return adapter_cls(data_dir)


def run(
    jurisdictions: list[str] | None = None,
    data_dir: str | Path = "data",
    output_dir: str | Path = "output",
    top_n: int = 10,
) -> pl.DataFrame:
    """Run the full pipeline for one or more jurisdictions.

    Parameters
    ----------
    jurisdictions : list of jurisdiction codes (e.g. ["nyc"]).
        Defaults to all registered jurisdictions.
    data_dir : root directory containing per-jurisdiction data.
    output_dir : where to write intermediate + result Parquet files.
    top_n : how many top landlords to export separately.

    Returns
    -------
    Full harm-score DataFrame (all owners, all requested jurisdictions).
    """
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if jurisdictions is None:
        jurisdictions = list(JURISDICTION_REGISTRY)

    all_violations: list[pl.LazyFrame] = []
    all_properties: list[pl.LazyFrame] = []
    all_contacts: list[pl.LazyFrame] = []

    for jur in jurisdictions:
        print(f"\n{'=' * 60}")
        print(f"Loading [{jur}]")
        print("=" * 60)

        adapter = _load_adapter(jur, data_dir)
        all_violations.append(adapter.load_violations())
        all_properties.append(adapter.load_properties())
        all_contacts.append(adapter.load_contacts())

    # ------------------------------------------------------------------
    # Write normalized union Parquet files for DuckDB
    # ------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("Writing normalized Parquet intermediates")
    print("=" * 60)

    violations = pl.concat(
        [lf.select(list(VIOLATIONS_SCHEMA)) for lf in all_violations],
    )
    properties = pl.concat(
        [lf.select(list(PROPERTIES_SCHEMA)) for lf in all_properties],
    )
    contacts = pl.concat(
        [lf.select(list(CONTACTS_SCHEMA)) for lf in all_contacts],
    )

    viol_df = violations.collect()
    prop_df = properties.collect()

    viol_df.write_parquet(
        output_dir / "violations.parquet", compression="zstd",
    )
    print(f"  violations.parquet: {len(viol_df):,} rows")

    prop_df.write_parquet(
        output_dir / "properties.parquet", compression="zstd",
    )
    print(f"  properties.parquet: {len(prop_df):,} rows")

    # ------------------------------------------------------------------
    # Ownership resolution
    # ------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("Resolving ownership networks")
    print("=" * 60)
    networks = resolve_ownership_networks(contacts)

    # Flatten the list[str] registration_ids into one row per pair
    owner_reg = networks.explode("registration_ids").select(
        "owner_id",
        "jurisdiction",
        "confidence",
        pl.col("registration_ids").alias("registration_id"),
    )
    owner_reg.write_parquet(
        output_dir / "owner_registrations.parquet", compression="zstd",
    )
    print(f"  owner_registrations.parquet: {len(owner_reg):,} rows")

    # ------------------------------------------------------------------
    # Harm scoring via DuckDB
    # ------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print("Computing harm scores (DuckDB)")
    print("=" * 60)
    harm_df = compute_harm_scores(output_dir)

    # Export
    harm_df.write_parquet(
        output_dir / "all_landlords_harm_scores.parquet", compression="zstd",
    )
    harm_df.head(top_n).write_parquet(
        output_dir / f"top_{top_n}_landlords.parquet", compression="zstd",
    )
    harm_df.head(top_n).write_csv(output_dir / f"top_{top_n}_landlords.csv")

    print(f"\nResults written to {output_dir}/")
    print(f"\nTop {top_n} landlords:")
    print(
        harm_df.head(top_n).select(
            "owner_id", "num_properties", "total_violations",
            "class_c_violations", "total_harm_score",
        )
    )

    return harm_df
