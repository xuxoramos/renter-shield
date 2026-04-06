#!/usr/bin/env python3
"""Generate or verify SHA-256 checksums for Parquet data files.

Usage:
    python make_manifest.py generate [--dir data] [--dir output]
    python make_manifest.py verify   [--dir data] [--dir output]

Each --dir produces a ``manifest.json`` inside that directory.
Defaults to both ``data/`` and ``output/`` if no --dir flags are given.

The manifest is a JSON object mapping relative filenames to their SHA-256
hex digests.  Only ``.parquet`` and ``.csv`` files are included.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

_EXTENSIONS = {".parquet", ".csv"}


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def generate(directories: list[Path]) -> None:
    for directory in directories:
        if not directory.is_dir():
            print(f"SKIP {directory}/ — not found")
            continue

        manifest: dict[str, str] = {}
        files = sorted(
            p for p in directory.iterdir()
            if p.is_file() and p.suffix in _EXTENSIONS
        )
        for path in files:
            digest = _hash_file(path)
            manifest[path.name] = digest
            print(f"  {digest[:12]}…  {path}")

        out = directory / "manifest.json"
        out.write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"  → {out} ({len(manifest)} files)\n")


def verify(directories: list[Path]) -> bool:
    all_ok = True
    for directory in directories:
        manifest_path = directory / "manifest.json"
        if not manifest_path.exists():
            print(f"SKIP {directory}/ — no manifest.json")
            continue

        manifest: dict[str, str] = json.loads(manifest_path.read_text())
        print(f"Verifying {directory}/ ({len(manifest)} files)")

        for name, expected in sorted(manifest.items()):
            path = directory / name
            if not path.exists():
                print(f"  MISSING  {name}")
                all_ok = False
                continue
            actual = _hash_file(path)
            if actual == expected:
                print(f"  OK       {name}")
            else:
                print(f"  MISMATCH {name}")
                print(f"           expected {expected}")
                print(f"           got      {actual}")
                all_ok = False

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["generate", "verify"],
        help="generate or verify manifest checksums",
    )
    parser.add_argument(
        "--dir", dest="dirs", action="append", type=Path,
        help="directories to process (default: data/ and output/)",
    )
    args = parser.parse_args()

    directories = args.dirs or [Path("data"), Path("output")]

    if args.command == "generate":
        generate(directories)
    else:
        ok = verify(directories)
        if not ok:
            print("\nVerification FAILED")
            sys.exit(1)
        print("\nAll files verified OK")


if __name__ == "__main__":
    main()
