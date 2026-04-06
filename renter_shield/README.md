# renter_shield — Design Decisions

Technical design rationale for the multi-jurisdiction landlord investigation
pipeline.  For project overview and harm score formula see the
[top-level README](../README.md).

## Time Horizon: 2022-01-01

All jurisdiction adapters share a single `MIN_DATE` defined in `config.py`.

**Why not earlier?**  COVID-19 caused most cities to pause non-emergency
housing inspections from roughly March 2020 through mid-2021:

| City         | Inspections resumed |
|--------------|---------------------|
| SF (DBI)     | ~March 2021         |
| Chicago      | ~April 2021         |
| NYC (HPD)    | ~May 2021           |
| Philadelphia | ~May 2021           |
| Boston (ISD) | ~June 2021          |

Including 2020–2021 data would introduce two distortions:

1. **False negatives in persistence** — the inspection gap breaks violation
   recurrence chains, making chronic offenders appear "clean" for 1–2 years
   when in reality no one inspected their buildings.
2. **Backlog surges in severity/density** — when inspections resumed in late
   2021, inspectors captured years of accumulated neglect in a single wave,
   artificially inflating scores for landlords inspected during the catch-up
   period relative to those inspected later.

By January 2022 every city had been fully operational for at least six months
and the initial backlog surge had cleared.

**Why not later (e.g. 2023)?**  The persistence component of the harm score
(10 % weight) measures repeated violation cycles at the same property.
Typical recurrence timelines:

- Fast-cycling (heat, vermin, leaks): 6–8 months per cycle.
- Slow-cycling (structural, building systems): 12–18 months per cycle.

A 2023 start date provides ~3.25 years — enough for fast-cycling issues but
only ~2 cycles for structural problems.  Starting in 2022 extends the window
to ~4.25 years, yielding ≥3 full cycles even for slow-cycling violations
while keeping data volumes manageable.

## Jurisdiction Adapters

Each adapter normalises local data into the universal schema defined in
`models.py` (violations, properties, contacts).  New cities are registered
in `config.py:JURISDICTION_REGISTRY`.

| Jurisdiction   | Data source | API            | BBL format           | Owner data |
|----------------|-------------|----------------|----------------------|------------|
| NYC            | HPD         | Socrata        | `{boro}{block}{lot}` | HPD registrations + contacts |
| Chicago        | Bldg Violations + Scofflaw | Socrata | `chi-{property_group}` | Scofflaw defendant_owner |
| Philadelphia   | L&I Violations + OPA | Carto SQL | `phl-{opa_account_num}` | OPA owner_1 / owner_2 |
| San Francisco  | DBI Complaints + Assessor Roll | Socrata | `sf-{block}{lot}` | None (assessor has property data but no owner names) |
| Boston         | Code Enforcement + Bldg Violations + Assessment | CKAN | `bos-{sam_id}` | Assessment OWNER + MAIL_* fields |
| Seattle        | SDCI Code Complaints & Violations | Socrata | `sea-{address}` | None (King County Assessor not on Socrata) |
| Pittsburgh     | PLI/DOMI/ES Violations + Allegheny County Assessments | CKAN (WPRDC) | `pit-{parcel_id}` | None (no owner names in assessments) |
| HUD REAC       | Multifamily Assisted Properties | ArcGIS FeatureServer | `hud-{PROPERTY_ID}` | Management agent org + contact |
| Los Angeles    | LADBS Code Enforcement Cases | Socrata | `la-{CSNumber}` | None |
| Austin         | Code Complaint Cases | Socrata | `austin-{PARCELID}` | None |
| Miami-Dade     | Code Compliance Violations + Building Violations | ArcGIS REST | `miami-{FOLIO}` | Building Violation VIOL_NAME (owner/agent) |
| Detroit        | Blight Tickets | ArcGIS REST | `det-{parcel_id}` | property_owner_name + mailing address |

### Deferred jurisdictions

- **Washington DC** — `opendata.dc.gov` is an ArcGIS Hub that federates
  across all public ArcGIS portals, not DC-specific data.  No dedicated
  housing violations dataset was found.  DC GIS MapServer offers 311 service
  requests but not housing code inspections.

- **Portland** — ArcGIS Hub portal (`gis-pdx.opendata.arcgis.com`)
  returned no code enforcement or housing violations datasets.  The BDS
  code enforcement cases page returns 404 as of April 2026.

- **Houston** — City open-data portal returns 404 for code enforcement
  endpoints as of April 2026.

- **Dallas** — Open data portal has no housing code violation dataset.

- **San Antonio** — Open data portal returns 404 as of April 2026.

- **Fort Worth** — Open data portal page returns 404 as of April 2026.

- **Baltimore** — ArcGIS Hub has only aggregated neighbourhood-level
  percentages of housing violations, not individual violation records.
  311 service request data exists but does not contain housing code
  inspections.

### HUD REAC score mapping

Unlike city-level adapters that ingest individual code violations, HUD REAC
provides a single physical inspection score (0–100) per property.  The adapter
converts each score into a synthetic violation record whose severity tier
reflects the federal inspection outcome:

| Score range | Tier | Meaning | Rationale |
|-------------|------|---------|----------|
| 0–59        | 1    | Critical | Property fails federal minimum standards |
| 60–79       | 2    | Serious  | Substandard conditions noted |
| 80–89       | 3    | Minor    | Adequate but improvement needed |
| 90–100      | 4    | Info     | Passing inspection |

REAC's `TROUBLED_CODE` field is mapped to violation status: `"N"` (not
troubled) → `"closed"`, any other value → `"open"`.  Management agent
organisation name and contact are used for the contacts table, enabling
cross-referencing with city-level landlord data.

Data source: HUD Multifamily Properties — Assisted (ArcGIS Feature Service),
~23,800 properties nationwide.  Only the most recent inspection per property
is published.

## Severity Mapping

Every adapter maps local violation codes/classes to a universal four-tier
scale (defined in `config.py:SEVERITY_POINTS`):

| Tier | Points | Meaning                    | Examples                                      |
|------|--------|----------------------------|-----------------------------------------------|
| 1    | 5.0    | Critical / immediately hazardous | NYC Class C, fire, lead, structural collapse |
| 2    | 2.5    | Serious / hazardous        | NYC Class B, plumbing, electrical, egress     |
| 3    | 1.0    | Minor / non-hazardous      | NYC Class A, signage, permits, trash          |
| 4    | 0.0    | Informational              | NYC Class I                                   |

Adapters without a formal classification system (Chicago, SF, Boston,
Philadelphia, Seattle, Pittsburgh, LA, Austin, Miami-Dade, Detroit) use
keyword heuristics on violation descriptions and
department/bureau fields.  The keyword lists are defined at the top of each
adapter module and can be reviewed/adjusted independently.

## Harm Score Weights

`severity (40%) + density (30%) + widespread (20%) + persistence (10%)`.

See the [top-level README](../README.md#harm-score) for the full formula,
component definitions, and weight justification.

## Owner Data Enrichment via County Assessor Records

Not all jurisdictions provide owner/contact data through their violation
datasets.  To fill these gaps we evaluated three options:

| Option | Source | Cost | Coverage |
|--------|--------|------|----------|
| Regrid API | Parcel + owner records | $375+/mo | Nationwide |
| ATTOM | Property + owner records | Enterprise pricing | Nationwide |
| Free county assessor data | Per-city open data portals | Free | Per jurisdiction |

**Decision:** Use free county assessor / assessment data.  The adapters
already abstract per-jurisdiction differences, so adding a city-specific
enrichment source is a natural fit without any architectural changes.

### Per-jurisdiction results

- **San Francisco** — Assessor Historical Secured Property Tax Rolls
  (`wv5m-vpq2`, Socrata).  212K records for roll year 2024 with 46 columns
  of property characteristics (year built, units, bedrooms, lot area, use
  code, assessed values).  **No owner name or mailing address fields.**
  Used only to enrich `load_properties()` with `year_built` and
  `units_residential` via block+lot join.  Contacts remain empty.

- **Boston** — Property Assessment FY2026 (CKAN resource `ee73430d`,
  184K records).  Includes `OWNER`, `MAIL_ADDRESSEE`, `MAIL_STREET_ADDRESS`,
  `MAIL_CITY`, `MAIL_STATE`, `MAIL_ZIP_CODE`, plus `YR_BUILT` and `PID`.
  Enriches both `load_properties()` (year built, units) and
  `load_contacts()` (owner name + mailing address) via PID join.  The
  `OWNER` field is mapped to `business_name` since it may contain trusts,
  LLCs, or combined names that cannot be reliably split into first/last.

## API Schema Changes

- **San Francisco DBI Complaints (`nbtm-fbw5`)** — as of April 2026 the
  Socrata schema no longer includes `item_sequence_number` or
  `nov_category_description`.  The adapter now uses `primary_key` for
  violation IDs and `code_violation_desc` + `unsafe_building` for severity
  classification.

## Access Control & Audit

### Design rationale

The data is derived entirely from public records, so the primary risk is
reputational misuse of algorithmic scores rather than data exfiltration.
A traditional approval-gated system would create bottlenecks for
investigators who need immediate access.

**Chosen approach: self-registration with audit logging.**

- Users register via a form (name, email, role) on first visit to either
  Streamlit app.  Access is granted immediately — no admin approval needed.
- A UUID token is issued at registration and doubles as an API key for
  programmatic access via the `X-API-Key` header.
- All page views (Streamlit) and API calls (FastAPI) are logged with
  user ID, path, and timestamp to `logs/audit.db` (SQLite).
- Tokens expire after 7 days (`LI_SESSION_EXPIRY_DAYS`), after which the
  user must re-register.

### Why SQLite?

- Zero cost — stdlib `sqlite3`, no new dependencies.
- Concurrent-safe for Streamlit's per-request model (WAL mode).
- Single file, easily backed up or downloaded for review.
- Queryable — supports ad-hoc access audits, unlike append-only CSV.
- Shared across both Streamlit apps and the API.

### Schema

```sql
users (id, name, email, role, scope, token, registered_at, ip)
page_views (id, user_id, scope, page, params, viewed_at)
api_calls (id, user_id, path, method, called_at)
```

### Scope enforcement

| Scope | Streamlit access | API access |
|-------|-----------------|------------|
| `renter` | Renter app only | `/renter/*` endpoints |
| `investigator` | Investigator app only | `/renter/*` + `/investigator/*` |

Legacy env-var keys (`LI_API_KEYS`) continue to work for the API and are
not subject to expiry.  They are intended for admin/CI use.
