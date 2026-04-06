"""CLI entry point for running the landlord investigation pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from renter_shield.config import JURISDICTION_REGISTRY
from renter_shield.pipeline import run


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the landlord harm-score pipeline."
    )
    parser.add_argument(
        "-j", "--jurisdictions",
        nargs="+",
        default=None,
        help=f"Jurisdiction codes to process (default: all). Available: {list(JURISDICTION_REGISTRY)}",
    )
    parser.add_argument(
        "-d", "--data-dir",
        type=Path,
        default=Path("data"),
        help="Root data directory (default: data/)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("output"),
        help="Output directory for results (default: output/)",
    )
    parser.add_argument(
        "-n", "--top-n",
        type=int,
        default=10,
        help="Number of top landlords to export (default: 10)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download raw data before processing (requires sodapy for NYC)",
    )

    args = parser.parse_args()

    jurisdictions = args.jurisdictions

    if args.download:
        import importlib
        from renter_shield.pipeline import _load_adapter

        for jur in jurisdictions or list(JURISDICTION_REGISTRY):
            adapter = _load_adapter(jur, args.data_dir)
            adapter.download()

    run(
        jurisdictions=jurisdictions,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        top_n=args.top_n,
    )


if __name__ == "__main__":
    main()
