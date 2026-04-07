# renter-shield

[![CI/CD](https://github.com/xuxoramos/renter-shield/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/xuxoramos/renter-shield/actions/workflows/ci-cd.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19418743.svg)](https://doi.org/10.5281/zenodo.19418743)
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Multi-jurisdiction landlord harm-score toolkit.  Downloads housing code
enforcement data from twelve U.S. cities and federal HUD inspections
(split by state), normalises it into a common schema, computes a composite
harm score per landlord, and serves the results via a dual-audience REST API
and two purpose-built Streamlit dashboards — one for renters and one for
investigators.

## Data Archive

Raw data and scored output are archived on Zenodo for reproducibility and
preservation:

**DOI**: [10.5281/zenodo.19418743](https://doi.org/10.5281/zenodo.19418743)
(concept DOI — always resolves to the latest version)

To restore data after cloning:

1. Download the archives from Zenodo.
2. Extract into `data/` and `output/`:
   ```bash
   tar xzf renter-shield-data-2026-04-04.tar.gz
   tar xzf renter-shield-output-2026-04-04.tar.gz
   ```
3. Verify integrity: `python make_manifest.py verify`

## Jurisdictions

| Jurisdiction   | Data source | API            | Owner data |
|----------------|-------------|----------------|------------|
| NYC            | HPD Violations + Registrations + Contacts | Socrata | HPD registrations + contacts |
| Chicago        | Building Violations + Scofflaw List | Socrata | Scofflaw defendant_owner (via address bridge to property_group) |
| Philadelphia   | L&I Violations + OPA Property | Carto SQL | OPA owner_1 / owner_2 |
| San Francisco  | DBI Complaints + Assessor Roll | Socrata | None (assessor has property data but no owner names) |
| Boston         | Code Enforcement + Building Violations + Assessment | CKAN | Assessment OWNER (via address bridge to violation sam_id) |
| Seattle        | SDCI Code Complaints & Violations | Socrata | None (King County Assessor not on Socrata) |
| Pittsburgh     | PLI/DOMI/ES Violations + PLI Permits + Allegheny County Assessments | CKAN (WPRDC) | None (no owner names in assessments) |
| HUD REAC       | Multifamily Assisted Properties | ArcGIS FeatureServer | Management agent org + contact (split by state: `hud_reac_ny`, `hud_reac_ca`, etc.) |
| Los Angeles    | LADBS Code Enforcement Cases | Socrata | None |
| Austin         | Code Complaint Cases | Socrata | None |
| Miami-Dade     | Code Compliance Violations + Building Violations | ArcGIS REST | Building Violation VIOL_NAME (owner/agent) |
| Detroit        | Blight Tickets | ArcGIS REST | property_owner_name + mailing address |

### Scored Jurisdictions

Only jurisdictions with owner/contact data produce scored landlords.
SF, Seattle, LA, and Austin have no owner data and contribute only
property/violation records.

| Jurisdiction | Scored owners | Addresses indexed |
|--------------|---------------|-------------------|
| NYC | 29,848 | 177K (87%) |
| Boston | 8,243 | 94K (100%) |
| Detroit | 6,417 | 80K (100%) |
| Philadelphia | 3,929 | 531K (100%) |
| HUD REAC (per state) | 1,528 | 23K (100%) |
| Miami-Dade | 260 | 61K (100%) |
| Pittsburgh | 135 | 48K (90%) |
| Chicago | 1 | 32K (100%) |
| LA | — | 85K (100%) |
| Austin | — | 28K (100%) |
| SF | — | 10K (100%) |
| Seattle | — | 28K (100%) |

HUD REAC data is split into per-state jurisdictions (e.g. `hud_reac_ny`,
`hud_reac_ca`) so ownership resolution and SVI percentile scoring operate
within each state rather than nationwide.

## Scoring Methodology

Scoring uses a two-stage pipeline: raw component computation via DuckDB SQL,
followed by an SVI-style percentile composite via Polars.

### Stage 1 — Raw Components (DuckDB)

Four raw metrics are computed per owner from Parquet files:

| Component | Description |
|-----------|-------------|
| **severity_score** | Weighted sum of violations by severity tier: Critical = 5 pts, Serious = 2.5 pts, Minor = 1 pt, Info = 0 |
| **density_score** | Total violations ÷ residential units — harm per tenant |
| **widespread_score** | Properties with violations ÷ total registered properties |
| **persistence_score** | Open violations ÷ total violations — failure to remediate |

A legacy weighted-linear composite (`total_harm_score`) is also computed
for backward compatibility:

```
total_harm_score = (severity × 0.4) + (density × 100 × 0.3) + (widespread × 100 × 0.2) + (persistence × 100 × 0.1)
```

### Stage 2 — SVI Composite (Polars)

Raw components are converted to a defensible 0–1 composite using the CDC
Social Vulnerability Index (SVI) methodology (Flanagan et al., 2011):

1. **Percentile pooling** — Each owner is assigned to a percentile pool.
   Jurisdictions with ≥ `MIN_JURISDICTION_SIZE` (50) scored owners use
   their own distribution.  Smaller jurisdictions are pooled together so
   ranks are not dominated by a single outlier.

2. **Percentile ranking** — Each raw component is ranked within its pool
   to a 0–1 scale (average tie-breaking).

3. **Theme aggregation** — Percentile-ranked components are grouped into
   three themes and averaged within-theme:

   | Theme | Components | Interpretation |
   |-------|-----------|----------------|
   | **Severity** | severity_score | How hazardous are the violations? |
   | **Portfolio** | density_score, widespread_score | How concentrated and widespread is the harm? |
   | **Compliance** | persistence_score | Does the landlord resolve violations? |

4. **Composite** — Theme scores are averaged into `svi_composite` (0–1).

5. **Likert mapping** — The composite is mapped to a 5-level rating:

   | Level | Composite range | Label | Indicator |
   |-------|----------------|-------|-----------|
   | 1 | 0.00 – 0.20 | Low concern | 🟢 |
   | 2 | 0.20 – 0.40 | Some concerns | 🟡 |
   | 3 | 0.40 – 0.60 | Moderate concerns | 🟠 |
   | 4 | 0.60 – 0.80 | Significant concerns | 🔴 |
   | 5 | 0.80 – 1.00 | Severe concerns | 🔴 |

The Likert rating is the renter-facing output.  Investigators see the full
composite, theme percentiles, and raw component breakdown.

#### Output Columns (24 total)

The scored Parquet file contains: `owner_id`, `jurisdiction`, `confidence`,
`num_properties`, `total_violations`, `class_c_violations`, `severity_score`,
`density_score`, `widespread_score`, `persistence_score`, `total_units`,
`unresolved_violations`, `total_harm_score`, `jur_n`, `_pctl_pool`,
`severity_score_pctl`, `density_score_pctl`, `widespread_score_pctl`,
`persistence_score_pctl`, `theme_severity`, `theme_portfolio`,
`theme_compliance`, `svi_composite`, `likert_level`, `likert_label`,
`likert_color`.

#### Scoring Distribution

As of 2026-04-06 (50,361 owners across 12 jurisdictions + HUD state splits):

| Level | Count | % |
|-------|-------|---|
| 1 — Low concern | 3,565 | 7.1% |
| 2 — Some concerns | 11,770 | 23.4% |
| 3 — Moderate concerns | 17,607 | 35.0% |
| 4 — Significant concerns | 15,280 | 30.3% |
| 5 — Severe concerns | 2,125 | 4.2% |

### Severity Tiers

| Tier | Points | Meaning | Examples |
|------|--------|---------|----------|
| 1 | 5.0 | Critical / immediately hazardous | NYC Class C, fire, lead, structural |
| 2 | 2.5 | Serious / hazardous | NYC Class B, plumbing, electrical |
| 3 | 1.0 | Minor / non-hazardous | NYC Class A, signage, trash |
| 4 | 0.0 | Informational | NYC Class I |

Adapters without formal classification (Chicago, SF, Boston, Philadelphia,
Seattle, Pittsburgh, LA, Austin, Miami-Dade, Detroit) use keyword heuristics
on violation descriptions.

### Ownership Resolution

A single person can appear on dozens of property registrations across a city.
To compute a harm score *per landlord* rather than *per property*, we must
group registrations that belong to the same real-world person.  This is
nontrivial because public records use free-text name fields with no unique
identifier.

**The problem — an everyday example.**  Imagine two completely different
people both named "Maria Garcia" who each own rental buildings in
Philadelphia.  One is a responsible landlord with zero violations.  The other
has serious code problems.  If we naïvely merge every registration filed
under "Maria Garcia" into one owner record, the responsible Maria Garcia
inherits the other's violations — a false accusation.  Conversely, imagine
one real landlord who files registrations as "John Smith", "John A. Smith",
and "John Smith Jr." to manage different properties from the same office at
100 Main Street.  A naïve exact-match would create three separate records
and undercount that person's true portfolio size and violation burden.

We address both problems with a four-layer matching system:

#### 1. Name Normalisation

Before comparing names, we standardise them:

- Uppercase everything (`john smith` → `JOHN SMITH`)
- Remove generational suffixes (`JR`, `SR`, `II`, `III`, `IV`, `ESQ`)
- Remove middle initials (`JOHN A SMITH` → `JOHN SMITH`)
- Collapse extra whitespace

This ensures "John A. Smith Jr." and "JOHN SMITH" are recognised as the
same candidate identity.

#### 2. Junk & Government Entity Filtering

Certain names are removed before matching because they don't represent
individual landlords:

- Government entities: "AUTH PHILADELPHIA HOUSING", "CITY OF …",
  "SCHOOL DISTRICT OF …", "US DEPT …"
- Placeholder entries: "#", single-character names, punctuation-only fields

In our data, "AUTH PHILADELPHIA HOUSING" alone appeared on 3,725
registrations across 2,065 addresses — clearly not a person.

#### 3. Confidence-Tiered Matching

After normalisation and filtering, each ownership group receives a
confidence rating based on how much evidence supports the identity link:

| Confidence | Rule | Meaning |
|------------|------|---------|
| **High** | Name matches AND ≤3 distinct business addresses (at least one shared) | Strong signal: same name operating from the same office(s). Very likely the same person. |
| **Medium** | Name matches, no address data available, but ≤3 addresses total | Plausible: uncommon name with a small footprint. Likely the same person, but no address corroboration. |
| **Low** | Address matches only (no name match) | Same office manages multiple properties, but could be different people at the same firm. |
| **Dropped** | Name matches but >3 distinct addresses | Collision risk: a common name appearing at many unrelated addresses is probably multiple different people. These groups are discarded entirely. |

##### Returning to the example

- **"Maria Garcia"** files registrations from 12 different addresses across
  Philadelphia → **dropped** (>3 addresses, likely a name collision).  Neither
  Maria Garcia is scored under a merged identity.
- **"John Smith" / "John A. Smith" / "John Smith Jr."** all normalise to
  `JOHN SMITH` and share the same business address at 100 Main Street →
  **high confidence**.  Their registrations are correctly merged into one owner.

#### 4. Business Name Grouping

Some jurisdictions (notably Boston) publish owner names in a single combined
field (e.g. "SMITH JOHN", "ACME TRUST LLC") rather than separate first/last
name columns.  When `first_name` and `last_name` are both empty but
`business_name` is set, we apply the same confidence-tiered matching on the
normalised business name.  This captures both person names stored in
business-name format and corporate entities (LLCs, trusts, realty companies)
that control multiple properties.

Junk filtering is applied identically — government entities, single-character
names, and trivial entries are excluded.

#### Minimum Registration Threshold

Groups with fewer than 3 registrations are excluded.  A person appearing on
only 2 registrations could easily be a coincidence for common names.
Requiring ≥3 registrations provides a stronger signal that the records
genuinely refer to the same landlord.

#### Impact

| Metric | Before (naïve matching) | After (confidence matching) |
|--------|------------------------|-----------------------------|
| Ownership groups | 144,212 | 51,080 |
| Owners scored | 91,418 | 50,347 |
| Jurisdictions scored | 3 | 8 (NYC, Boston, Philadelphia, HUD REAC, Chicago, Miami-Dade, Detroit, Pittsburgh) |

The 52% reduction in scored owners represents false positives — name
collisions and government entities that were incorrectly treated as
individual landlords.  The addition of business-name grouping and
registration-ID bridge fixes (Boston sam_id↔PID, Chicago address↔property_group)
brought two previously unscored jurisdictions online.  Every record carries
a `confidence` field (`high`, `medium`, or `low`) in both the Parquet output
and all API/dashboard responses, alongside a disclaimer that independent
verification is required.

## Architecture

```
renter_shield/
├── config.py           # MIN_DATE, severity points, SVI themes, Likert thresholds, registry
├── models.py           # Universal schema (violations, properties, contacts)
├── jurisdictions/
│   ├── base.py         # Abstract adapter: download(), load_violations/properties/contacts()
│   ├── nyc.py          # HPD via Socrata
│   ├── chicago.py      # Building Violations + Scofflaw via Socrata
│   ├── philadelphia.py # L&I + OPA via Carto SQL
│   ├── sf.py           # DBI Complaints + Assessor via Socrata
│   ├── boston.py        # Code Enforcement + Assessment via CKAN
│   ├── seattle.py      # SDCI Code Complaints via Socrata
│   ├── pittsburgh.py   # PLI Violations + County Assessments via CKAN (WPRDC)
│   ├── hud_reac.py     # HUD Multifamily via ArcGIS FeatureServer
│   ├── la.py           # LADBS Code Enforcement Cases via Socrata
│   ├── austin.py       # Code Complaint Cases via Socrata
│   ├── miami.py        # Code Compliance + Building Violations via ArcGIS REST
│   └── detroit.py      # Blight Tickets via ArcGIS REST
├── ownership.py        # Confidence-tiered owner network resolution
├── scoring.py          # Two-stage scoring: DuckDB raw components + Polars SVI composite
├── pdf_report.py       # Printable PDF property report (fpdf2)
├── pipeline.py         # Orchestrates download → load → resolve → score
├── audit.py            # SQLite-backed user registration, token auth, audit logging
├── cli.py              # Command-line interface
└── api.py              # Dual-audience FastAPI: /renter/ and /investigator/ routes
streamlit_renter.py     # Renter dashboard: address search → property detail (port 8501)
streamlit_investigator.py  # Investigator dashboard: overview → jurisdiction → owner (port 8502)
deploy/
├── Dockerfile          # Python 3.12-slim, FastAPI + 2 Streamlit apps
├── docker-compose.yml  # app + nginx reverse proxy (ports 8000, 8501, 8502)
├── entrypoint.sh       # Runs uvicorn + both Streamlit apps
├── nginx.conf          # Rate-limited proxy, TLS-ready
└── landing/            # Static landing page served at /about
    └── index.html
logs/
└── audit.db            # SQLite user registrations + access audit trail (gitignored)
```

**Data flow:** Adapters download raw data as Parquet → `load_*()` methods
return Polars LazyFrames in the universal schema → DuckDB queries Parquet
files directly for scoring and API serving.

## Installation

```bash
pip install .
# or in development
pip install -e .
```

## Usage

```bash
# Download data for all jurisdictions
renter-shield download --all

# Download a single jurisdiction
renter-shield download --jurisdiction nyc

# Run the scoring pipeline
renter-shield score

# Start the API server (dual-audience, scoped keys)
export LI_API_KEYS="inv-key:investigator,renter-key:renter"
uvicorn renter_shield.api:app --host 0.0.0.0 --port 8000

# Start the renter Streamlit dashboard (port 8501)
streamlit run streamlit_renter.py --server.port 8501

# Start the investigator Streamlit dashboard (port 8502)
streamlit run streamlit_investigator.py --server.port 8502
```

## Streamlit Dashboards

Two separate Streamlit apps serve different audiences from the same data.

### Renter App (`streamlit_renter.py` — port 8501)

Address-first interface for prospective tenants.  Shows Likert ratings
instead of raw scores.  No owner IDs or confidence tiers are exposed.
Requires self-registration before access (name, email, role).

| Page | URL | Description |
|------|-----|-------------|
| **Address Search** | `/` | Search by address + jurisdiction, results show Likert rating per property and owner signal |
| **Property Detail** | `/?page=property&bbl=...` | Violation timeline, Likert rating, owner Likert rating (if available), rating explainer, downloadable PDF report |
| **Owner Detail** | `/?page=owner&owner=...` | Landlord rating, properties managed, total violations, property list |

The renter app includes a **Know Your Rights** sidebar with links to tenant
protection resources, complaint hotlines, and legal aid organizations
for each covered city.

### Investigator App (`streamlit_investigator.py` — port 8502)

Owner-centric harm-score explorer for housing investigations.  Full access
to SVI composite, theme percentiles, legacy scores, and confidence tiers.
Requires self-registration before access; all page views are logged to the
audit database for accountability.

| Page | URL | Description |
|------|-----|-------------|
| **Overview** | `/` | Jurisdiction cards with summary stats, cross-jurisdiction search |
| **Jurisdiction** | `/?page=jurisdiction&jur=boston` | Ranked owner table with Likert badge + confidence badge, filters, score distribution chart |
| **Owner Detail** | `/?page=owner&owner=...` | SVI composite, Likert rating, theme percentiles (severity, portfolio, compliance), legacy score breakdown chart + table, confidence callout |

## API Endpoints

The API serves two audiences from the same data via route prefixes and
scoped API keys.

### Authentication

All data endpoints require an `X-API-Key` header.  Keys can come from two
sources:

1. **Environment / file** — `LI_API_KEYS` env var (comma-separated) or
   `api_keys.txt` (one per line).  Format: `key:scope` (bare key defaults
   to `investigator`).
2. **Self-registration** — users who register via either Streamlit app
   receive a UUID token that also works as an API key.  Tokens are stored
   in `logs/audit.db` (SQLite) and expire after 90 days (configurable via
   `LI_SESSION_EXPIRY_DAYS`).

Both sources are checked on every request.  All API calls made with
SQLite-registered tokens are logged to the audit database.

| Scope | Access |
|-------|--------|
| `renter` | `/renter/*` only — returns Likert ratings, no owner_id or score breakdowns |
| `investigator` | `/renter/*` + `/investigator/*` — full detail including SVI composite, theme percentiles, confidence |

### Renter routes (`/renter/`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/renter/address/search` | Address search — returns Likert rating + owner signal (rating, properties managed, total violations) |
| GET | `/renter/property/{bbl}` | Property detail — Likert rating, stripped violations (no violation_id), owner Likert if available |

### Investigator routes (`/investigator/`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/investigator/jurisdictions` | Summary stats + confidence breakdown per city |
| GET | `/investigator/jurisdictions/{jur}/landlords` | Paginated owners for one jurisdiction (filter by min_score, min_properties, name) |
| GET | `/investigator/address/search` | Address search — full detail with scores |
| GET | `/investigator/property/{bbl}` | Property detail — full violations + scores |
| GET | `/investigator/landlords/search` | Cross-jurisdiction search (filter by name, jurisdiction, min_score) |
| GET | `/investigator/landlords/{owner_id}` | Full detail: SVI composite, theme percentiles, legacy score breakdown, confidence |

### Unauthenticated

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |

## Deployment

See [DEPLOY.md](DEPLOY.md) for Hetzner VPS deployment instructions
(Docker + nginx + TLS).  The nginx config also serves a static landing page
at `/about` (source: `deploy/landing/index.html`).

## Configuration

All tunables live in `config.py`:

- `MIN_DATE` — earliest violation date (default: `2022-01-01`).
  Post-COVID resumption avoids inspection-gap distortions.
- `SEVERITY_POINTS` — point values per tier
- `SCORE_WEIGHTS` — legacy composite weights (severity, density, widespread, persistence)
- `RATIO_SCALE` — multiplier for ratio components in legacy composite (default: 100)
- `MIN_JURISDICTION_SIZE` — minimum owners for a jurisdiction to get its own
  percentile distribution (default: 50). Smaller jurisdictions are pooled.
- `SVI_THEMES` — theme-to-component mapping for the SVI composite
- `LIKERT_SCALE` — 5-level thresholds on the 0-1 SVI composite
- `JURISDICTION_REGISTRY` — maps short codes to adapter classes
- `LI_SESSION_EXPIRY_DAYS` — days before a self-registered token expires
  (default: 90)
- `LI_AUDIT_DIR` — directory for the audit SQLite database (default: `logs/`)

## Design Decisions

See [renter_shield/README.md](renter_shield/README.md) for
detailed rationale on time horizon selection, severity mapping, HUD REAC
score conversion, and owner data enrichment strategy.
