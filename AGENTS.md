# Project Guidelines

## Purpose

Toolkit for scoring U.S. landlords by housing code violation severity across
multiple jurisdictions. Built for the New York Office of the Attorney General
(NYOAG) investigation. The companion analysis repo
(`nyoag-landlord-investigation`) consumes this package for notebooks and
evidentiary snapshots.

## Architecture

- **Adapter pattern**: each city is a `JurisdictionAdapter` subclass in
  `renter_shield/jurisdictions/`. Adapters download raw data,
  normalize it into the universal schema (`models.py`), and return
  `polars.LazyFrame`s. New cities only need a new adapter + a one-line
  entry in `config.py:JURISDICTION_REGISTRY`.
- **Universal schema**: three tables — violations, properties, contacts —
  defined in `models.py`. All adapters must produce these columns; extra
  columns are ignored downstream.
- **Scoring via DuckDB**: DuckDB reads Parquet files directly at query time.
  No persistent `.duckdb` file — Parquet is the storage layer.
- **FastAPI server** (`api.py`): read-only queries against scored Parquet
  output. API key auth via header.
- **CLI** (`cli.py`): `download`, `score` subcommands. Data dir defaults to
  `data/` (relative, gitignored).

## Code Style

- Python 3.11+. Type hints on public functions.
- **Polars** for all DataFrame work — never pandas. Use `LazyFrame` where
  possible; `.collect()` only at boundaries (file write, API response).
- **stdlib `urllib`** for HTTP downloads — no `requests` or `httpx` dep.
  Some adapters use `sodapy` for Socrata but it's optional.
- Parquet files use zstd compression.
- Severity mapping keywords are defined at the top of each adapter. When
  adding or changing keywords, keep them sorted and comment the rationale.

## Key Constraints

- `MIN_DATE = "2022-01-01"` in `config.py` — do not change without reading
  the COVID-era inspection-gap rationale in the comment above it and in
  `renter_shield/README.md § Time Horizon`.
- `SCORE_WEIGHTS` must sum to 1.0.
- BBL formats are jurisdiction-specific (e.g. `sf-{block}{lot}`,
  `bos-{sam_id}`). The BBL is the primary join key across all three tables.
- Adapters that lack owner data return an empty `LazyFrame` with the correct
  `CONTACTS_SCHEMA` columns — never `None` or missing columns.
- HUD REAC is federal, not city-level. It produces synthetic violation
  records from inspection scores, not individual code violations.

## Build and Test

```bash
pip install -e ".[dev]"       # install with dev deps
pytest                        # run tests
ruff check .                  # lint
```

## Adding a New Jurisdiction

1. Create `renter_shield/jurisdictions/<city>.py` subclassing
   `JurisdictionAdapter`.
2. Implement `download()`, `load_violations()`, `load_properties()`,
   `load_contacts()`.
3. Map local violation codes to severity tiers 1-4 using keyword heuristics.
4. Add one entry to `JURISDICTION_REGISTRY` in `config.py`.
5. Verify with: `python -c "from renter_shield.jurisdictions.<city> import <Cls>; print('OK')"`.

## Conventions

- Data files go in `data/` (gitignored). Adapters write
  `<jurisdiction>_<dataset>.parquet`.
- Never commit data. The analysis repo handles archival + checksums.
- Adapter `download()` methods should be idempotent — overwrite existing
  files, don't append.
- When an open-data API changes schema, update the adapter and note the
  change in `renter_shield/README.md`.
