"""CLI entry point for running the landlord investigation pipeline."""

from __future__ import annotations

import argparse
import json
import sys
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
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any jurisdiction fails schema validation "
             "(default: drop drifted jurisdictions and continue)",
    )

    args = parser.parse_args()

    jurisdictions = args.jurisdictions or list(JURISDICTION_REGISTRY)

    if args.download:
        from renter_shield.pipeline import _load_adapter

        jur_list = jurisdictions

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

    # ------------------------------------------------------------------
    # Preflight schema validation — flag drift BEFORE the scoring step
    # ------------------------------------------------------------------
    from renter_shield.pipeline import validate_jurisdictions

    problems = validate_jurisdictions(jurisdictions, args.data_dir)
    healthy = [j for j in jurisdictions if j not in problems]

    if problems:
        print(f"\n{'=' * 60}")
        print("⚠  SCHEMA DRIFT DETECTED")
        print("=" * 60)
        for jur, issues in problems.items():
            print(f"  ✗ {jur}")
            for issue in issues:
                print(f"      - {issue}")

        # Persist a machine-readable report for CI to gate on.
        args.output_dir.mkdir(parents=True, exist_ok=True)
        report_path = args.output_dir / "schema_drift.json"
        report_path.write_text(json.dumps(problems, indent=2))
        print(f"\nDrift report written to {report_path}")

        if args.strict:
            print("\n--strict set: aborting without scoring.")
            sys.exit(1)

        if not healthy:
            print("\nNo healthy jurisdictions remain. Aborting.")
            sys.exit(1)

        print(f"\nContinuing with {len(healthy)} healthy jurisdiction(s): {healthy}")

    run(
        jurisdictions=healthy,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        top_n=args.top_n,
    )

    # Flag the run as failed if drift occurred, even though healthy
    # jurisdictions were still scored and exported.
    if problems:
        sys.exit(1)


if __name__ == "__main__":
    main()
