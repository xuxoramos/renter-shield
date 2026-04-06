# Next Steps

## ~~1. Data Download Test~~ ✅

Completed 2026-04-04.  All 8 adapters (NYC, Chicago, Philadelphia, SF,
Boston, Seattle, Pittsburgh, HUD REAC) download successfully.  Pagination
added for all Socrata/CKAN/ArcGIS sources; geographic coverage expanded
to full city boundaries.  See `data/` for 16 Parquet files.

## ~~2. DuckDB Scoring Engine~~ ✅

Completed 2026-04-04.  `scoring.py` rewritten to use a single DuckDB SQL
query over Parquet intermediates (`output/violations.parquet`,
`output/properties.parquet`, `output/owner_registrations.parquet`).
`pipeline.py` updated to write normalized intermediates before scoring.
91,418 owners scored across NYC, Philadelphia, and HUD REAC (the
jurisdictions with contacts/ownership data).

## 3. Zenodo Archival

Both repos need archival — raw source data in this tool repo is equally at
risk of going dark if open-data portals are taken offline.

### 3a. This repo (renter-shield)

Archive the raw downloads (`data/`) and scored output (`output/`):

1. Run `python make_manifest.py generate` to record SHA-256 checksums into
   `data/manifest.json` and `output/manifest.json`.
2. Zip both directories:
   ```bash
   zip -r renter-shield-data-2026-04-04.zip data/ -x 'data/manifest.json'
   zip -r renter-shield-output-2026-04-04.zip output/ -x 'output/manifest.json'
   ```
3. Upload both zips to [Zenodo](https://zenodo.org/).  Use metadata:
   - **Title**: Landlord Investigator — Raw Housing Code Violation Data
     (8 U.S. Jurisdictions, 2022–2026)
   - **Type**: Dataset
   - **License**: CC-BY-4.0 (the underlying open data is public)
   - **Keywords**: housing, code enforcement, violations, landlord, NYOAG
4. Record the DOI in this repo's README.
5. Commit only `data/manifest.json` and `output/manifest.json` (not the
   data files — they stay gitignored).
6. Anyone restoring runs `python make_manifest.py verify` to confirm
   integrity after downloading from Zenodo.

### 3b. Analysis repo (nyoag-landlord-investigation)

Same process for the analysis repo's notebook outputs and derived datasets.
That repo commits only `data/manifest.json`; actual data files are gitignored
but archived externally on Zenodo.
