#!/usr/bin/env python3
"""Upload a new version of the Renter Shield dataset to Zenodo.

Requires:
    ZENODO_TOKEN  — personal access token with deposit:write scope.

Usage:
    # Dry run — build archives, show what would be uploaded:
    python upload_zenodo.py --dry-run

    # Real upload to production Zenodo:
    python upload_zenodo.py

    # Sandbox (for testing):
    python upload_zenodo.py --sandbox

The script:
  1. Regenerates SHA-256 manifests for data/ and output/.
  2. Creates dated tar.gz archives for both directories.
  3. Creates a new version of the existing Zenodo deposit (concept record).
  4. Uploads data archive, output archive, and a source snapshot.
  5. Updates metadata (title, description, version, keywords).
  6. Publishes the new version.

The previous version's DOI remains valid.  Both versions share the
concept DOI which always resolves to the latest.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tarfile
from datetime import date, timezone, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONCEPT_RECORD_ID = "19418743"
DEPOSIT_ID = "19418744"  # published version — newversion API needs this, not concept ID
ZENODO_API = "https://zenodo.org/api"
SANDBOX_API = "https://sandbox.zenodo.org/api"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

TODAY = date.today().isoformat()

# Files/dirs excluded from the source archive
SOURCE_EXCLUDES = {
    ".venv", ".git", "__pycache__", "*.egg-info", "data", "output",
    "logs", "*.tar.gz", ".env", ".ruff_cache", ".pytest_cache",
    "htmlcov", ".coverage", "node_modules",
}

# ---------------------------------------------------------------------------
# Metadata for the new version
# ---------------------------------------------------------------------------

VERSION = "0.2.0"

DESCRIPTION = """\
<p><strong>Renter Shield v{version}</strong> — multi-jurisdiction landlord
harm-score toolkit for the NYOAG housing investigation.</p>

<h3>What changed in v{version}</h3>
<ul>
  <li><strong>4 new jurisdictions</strong>: Austin, Miami-Dade, Detroit,
      Los Angeles (total: 12 cities + HUD REAC federal data).</li>
  <li><strong>Revised scoring</strong>: SVI-style composite harm score with
      confidence-tiered ownership resolution.</li>
  <li><strong>Access control</strong>: SQLite-backed self-registration with
      audit logging for both API and Streamlit dashboards.</li>
  <li><strong>Package rename</strong>: <code>landlord-investigator</code> →
      <code>renter-shield</code> (all imports, CLI, docs).</li>
</ul>

<h3>Archive contents</h3>
<ul>
  <li><code>renter-shield-data-{today}.tar.gz</code> — raw downloaded
      Parquet files from all jurisdiction adapters ({n_data} files).</li>
  <li><code>renter-shield-output-{today}.tar.gz</code> — scored output:
      harm scores, owner registrations, properties, violations, top-N
      reports ({n_output} files).</li>
  <li><code>renter-shield-source-{today}.tar.gz</code> — full source
      tree (Python package, Streamlit apps, Dockerfile, deployment
      config).</li>
</ul>

<p>Each archive directory contains a <code>manifest.json</code> with
SHA-256 checksums.  Verify with:
<code>python make_manifest.py verify</code></p>
""".strip()

METADATA_TEMPLATE: dict = {
    "title": "Renter Shield — Multi-Jurisdiction Landlord Harm-Score Dataset",
    "upload_type": "dataset",
    "description": "",  # filled at runtime
    "version": VERSION,
    "creators": [
        {"name": "Ramos, Jesús", "affiliation": "NYOAG"},
    ],
    "keywords": [
        "housing", "code enforcement", "landlord", "harm score",
        "open data", "NYOAG", "renter shield",
    ],
    "license": "cc-by-4.0",
    "related_identifiers": [
        {
            "identifier": "https://github.com/xuxoramos/renter-shield",
            "relation": "isSupplementTo",
            "scheme": "url",
        }
    ],
    "notes": (
        "Evidentiary data archive for the New York Office of the Attorney "
        "General housing investigation.  See README.md in the source "
        "archive for full documentation."
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_call(
    base_url: str,
    path: str,
    token: str,
    method: str = "GET",
    json_body: dict | None = None,
    data: bytes | None = None,
    content_type: str | None = None,
    filename: str | None = None,
) -> dict | None:
    """Thin wrapper around urllib for Zenodo REST API calls."""
    url = f"{base_url}{path}"
    if "?" in url:
        url += f"&access_token={token}"
    else:
        url += f"?access_token={token}"

    headers: dict[str, str] = {}
    body: bytes | None = None

    if json_body is not None:
        body = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    elif data is not None:
        headers["Content-Type"] = content_type or "application/octet-stream"
        body = data

    req = Request(url, data=body, headers=headers, method=method)

    try:
        with urlopen(req) as resp:  # noqa: S310 — trusted Zenodo URL
            resp_body = resp.read()
            if resp_body:
                return json.loads(resp_body)
            return None
    except HTTPError as exc:
        err_body = exc.read().decode(errors="replace")
        print(f"HTTP {exc.code} on {method} {path}:\n{err_body}", file=sys.stderr)
        sys.exit(1)


def _count_files(directory: Path, extensions: set[str] = {".parquet", ".csv"}) -> int:
    if not directory.is_dir():
        return 0
    return sum(1 for p in directory.iterdir() if p.suffix in extensions)


def _build_archive(name: str, src_dir: Path) -> Path:
    """Create a tar.gz archive of a directory (data or output)."""
    archive_path = PROJECT_ROOT / f"{name}-{TODAY}.tar.gz"
    print(f"  Building {archive_path.name} from {src_dir.name}/ ...")
    with tarfile.open(archive_path, "w:gz") as tar:
        for p in sorted(src_dir.iterdir()):
            if p.is_file():
                tar.add(p, arcname=f"{src_dir.name}/{p.name}")
    size_mb = archive_path.stat().st_size / (1 << 20)
    print(f"  → {archive_path.name}  ({size_mb:.1f} MB)")
    return archive_path


def _build_source_archive() -> Path:
    """Create a tar.gz of the source tree, excluding data/output/venv."""
    archive_path = PROJECT_ROOT / f"renter-shield-source-{TODAY}.tar.gz"
    print(f"  Building {archive_path.name} ...")

    def _filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(info.name).parts
        for part in parts:
            for excl in SOURCE_EXCLUDES:
                if excl.startswith("*"):
                    if part.endswith(excl[1:]):
                        return None
                elif part == excl:
                    return None
        return info

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(PROJECT_ROOT, arcname="renter-shield", filter=_filter)
    size_mb = archive_path.stat().st_size / (1 << 20)
    print(f"  → {archive_path.name}  ({size_mb:.1f} MB)")
    return archive_path


def _upload_file(base_url: str, bucket_url: str, token: str, filepath: Path) -> None:
    """Upload a single file to a Zenodo deposit bucket."""
    fname = filepath.name
    # bucket_url is the full URL; we need to append the filename
    url = f"{bucket_url}/{fname}?access_token={token}"
    print(f"  Uploading {fname} ({filepath.stat().st_size / (1 << 20):.1f} MB) ...")

    file_data = filepath.read_bytes()
    req = Request(url, data=file_data, method="PUT")
    req.add_header("Content-Type", "application/octet-stream")
    try:
        with urlopen(req) as resp:  # noqa: S310
            resp.read()
        print(f"  ✓ {fname}")
    except HTTPError as exc:
        err_body = exc.read().decode(errors="replace")
        print(f"  ✗ {fname}: HTTP {exc.code}\n{err_body}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build archives and show metadata, but do not upload.",
    )
    parser.add_argument(
        "--sandbox", action="store_true",
        help="Upload to sandbox.zenodo.org instead of production.",
    )
    parser.add_argument(
        "--skip-manifests", action="store_true",
        help="Skip regenerating manifests (use if already current).",
    )
    args = parser.parse_args()

    base_url = SANDBOX_API if args.sandbox else ZENODO_API
    token = os.environ.get("ZENODO_TOKEN", "")
    if not token and not args.dry_run:
        print("ERROR: Set ZENODO_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    # 1. Regenerate manifests
    if not args.skip_manifests:
        print("\n[1/6] Regenerating manifests ...")
        subprocess.run(
            [sys.executable, "make_manifest.py", "generate"],
            cwd=PROJECT_ROOT,
            check=True,
        )
    else:
        print("\n[1/6] Skipping manifest generation (--skip-manifests)")

    # 2. Build archives
    print("\n[2/6] Building archives ...")
    data_archive = _build_archive("renter-shield-data", DATA_DIR)
    output_archive = _build_archive("renter-shield-output", OUTPUT_DIR)
    source_archive = _build_source_archive()
    archives = [data_archive, output_archive, source_archive]

    # 3. Prepare metadata
    n_data = _count_files(DATA_DIR)
    n_output = _count_files(OUTPUT_DIR)
    description = DESCRIPTION.format(
        version=VERSION,
        today=TODAY,
        n_data=n_data,
        n_output=n_output,
    )
    metadata = {**METADATA_TEMPLATE, "description": description}

    print(f"\n[3/6] Metadata prepared")
    print(f"  Title:   {metadata['title']}")
    print(f"  Version: {metadata['version']}")
    print(f"  Files:   {', '.join(a.name for a in archives)}")

    if args.dry_run:
        print("\n[DRY RUN] Would upload the following:")
        for a in archives:
            print(f"  • {a.name}  ({a.stat().st_size / (1 << 20):.1f} MB)")
        print(f"\n  Metadata:\n{json.dumps(metadata, indent=2)}")
        print("\nDone (dry run). No changes made to Zenodo.")
        return

    # 4. Create new version of existing deposit
    print(f"\n[4/6] Creating new version of deposit {DEPOSIT_ID} ...")
    result = _api_call(
        base_url,
        f"/deposit/depositions/{DEPOSIT_ID}/actions/newversion",
        token,
        method="POST",
    )
    # The response contains a link to the new draft
    new_version_url = result["links"]["latest_draft"]
    new_id = new_version_url.rstrip("/").split("/")[-1]
    print(f"  New draft deposit: {new_id}")

    # Get the new draft to find the bucket URL
    draft = _api_call(base_url, f"/deposit/depositions/{new_id}", token)
    bucket_url = draft["links"]["bucket"]

    # Delete any files inherited from the previous version
    for f in draft.get("files", []):
        print(f"  Removing inherited file: {f['filename']}")
        _api_call(
            base_url,
            f"/deposit/depositions/{new_id}/files/{f['id']}",
            token,
            method="DELETE",
        )

    # 5. Upload new files
    print(f"\n[5/6] Uploading files ...")
    for archive in archives:
        _upload_file(base_url, bucket_url, token, archive)

    # 6. Update metadata and publish
    print(f"\n[6/6] Updating metadata and publishing ...")
    _api_call(
        base_url,
        f"/deposit/depositions/{new_id}",
        token,
        method="PUT",
        json_body={"metadata": metadata},
    )

    _api_call(
        base_url,
        f"/deposit/depositions/{new_id}/actions/publish",
        token,
        method="POST",
    )

    print(f"\n  Published! New version DOI will appear shortly.")
    print(f"  Concept DOI (always latest): https://doi.org/10.5281/zenodo.{CONCEPT_RECORD_ID}")
    print(f"  Direct link: {base_url.replace('/api', '')}/records/{new_id}")


if __name__ == "__main__":
    main()
