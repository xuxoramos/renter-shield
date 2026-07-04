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
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Download jurisdictions in parallel (use with --download)",
    )

    args = parser.parse_args()

    jurisdictions = args.jurisdictions

    if args.download:
        from renter_shield.pipeline import _load_adapter

        jur_list = jurisdictions or list(JURISDICTION_REGISTRY)

        if args.parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _download_one(jur: str) -> str:
                adapter = _load_adapter(jur, args.data_dir)
                adapter.download()
                return jur

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(_download_one, j): j for j in jur_list}
                for fut in as_completed(futures):
                    jur = futures[fut]
                    try:
                        fut.result()
                        print(f"✓ {jur} download complete")
                    except Exception as exc:
                        print(f"✗ {jur} download failed: {exc}")
        else:
            for jur in jur_list:
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
