"""Harm score engine — DuckDB scoring on normalized Parquet files.

DuckDB reads Parquet directly at query time — no persistent .duckdb file.

Two-stage scoring:
  1. **Raw components** via DuckDB SQL: severity, density, widespread,
     persistence — plus the legacy weighted-linear composite
     (``total_harm_score``).
  2. **SVI-style composite** via Polars: within-jurisdiction percentile
     ranking of raw components, grouped into themes, averaged into a
     0-1 composite (``svi_composite``), and mapped to a 5-level Likert
     rating (``likert_level``, ``likert_label``).

See config.py for weight values, theme definitions, and Likert thresholds.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from renter_shield.config import (
    LIKERT_SCALE,
    MIN_JURISDICTION_SIZE,
    RATIO_SCALE,
    SCORE_WEIGHTS,
    SEVERITY_POINTS,
    SVI_THEMES,
)


def compute_harm_scores(output_dir: Path) -> pl.DataFrame:
    """Score every owner using DuckDB SQL on Parquet files.

    Expects these files in *output_dir*:
      - ``violations.parquet``          (universal schema)
      - ``properties.parquet``          (universal schema)
      - ``owner_registrations.parquet`` (owner_id, jurisdiction, registration_id)

    Returns
    -------
    DataFrame with columns:
        owner_id, jurisdiction, num_properties, total_violations,
        class_c_violations, severity_score, density_score, widespread_score,
        persistence_score, total_units, unresolved_violations, total_harm_score
    """
    viol_path = str((output_dir / "violations.parquet").resolve())
    prop_path = str((output_dir / "properties.parquet").resolve())
    owner_path = str((output_dir / "owner_registrations.parquet").resolve())

    # Config values injected into SQL
    s = {t: float(SEVERITY_POINTS[t]) for t in (1, 2, 3, 4)}
    w = SCORE_WEIGHTS
    scale = float(RATIO_SCALE)

    con = duckdb.connect()  # in-memory — no persistent file

    result = con.execute(f"""
        -- 1. Total registrations per owner (denominator for widespread)
        WITH owner_totals AS (
            SELECT
                owner_id,
                jurisdiction,
                MAX(confidence) AS confidence,
                COUNT(DISTINCT registration_id) AS num_properties
            FROM read_parquet('{owner_path}')
            GROUP BY owner_id, jurisdiction
        ),

        -- 2. Distinct properties per owner with residential units
        --    (each property counted once → correct density denominator)
        owner_prop_units AS (
            SELECT DISTINCT
                o.owner_id,
                p.bbl,
                COALESCE(p.units_residential, 1) AS units_residential
            FROM read_parquet('{owner_path}') o
            JOIN read_parquet('{prop_path}') p
                ON o.registration_id = p.registration_id
        ),
        owner_units AS (
            SELECT
                owner_id,
                GREATEST(SUM(units_residential), 1) AS total_units
            FROM owner_prop_units
            GROUP BY owner_id
        ),

        -- 3. Attribute each violation to its owner(s) via
        --    violation.bbl → property.bbl → owner.registration_id
        viol_owner AS (
            SELECT
                o.owner_id,
                v.bbl,
                v.severity_tier,
                v.status
            FROM read_parquet('{viol_path}') v
            JOIN read_parquet('{prop_path}') p ON v.bbl = p.bbl
            JOIN read_parquet('{owner_path}') o
                ON p.registration_id = o.registration_id
        ),

        -- 4. Aggregate violation metrics per owner
        viol_metrics AS (
            SELECT
                owner_id,
                COUNT(*)                                           AS total_violations,
                SUM(CASE severity_tier
                        WHEN 1 THEN {s[1]}
                        WHEN 2 THEN {s[2]}
                        WHEN 3 THEN {s[3]}
                        ELSE        {s[4]}
                    END)                                           AS severity_score,
                COUNT(CASE WHEN severity_tier = 1 THEN 1 END)     AS class_c_violations,
                COUNT(CASE WHEN status = 'open'   THEN 1 END)     AS unresolved_violations,
                COUNT(DISTINCT bbl)                                AS unique_bbls
            FROM viol_owner
            GROUP BY owner_id
        )

        -- 5. Final score assembly
        SELECT
            vm.owner_id,
            ot.jurisdiction,
            ot.confidence,
            ot.num_properties,
            vm.total_violations,
            vm.class_c_violations,
            vm.severity_score,
            vm.total_violations::DOUBLE / ou.total_units                       AS density_score,
            vm.unique_bbls::DOUBLE / ot.num_properties                         AS widespread_score,
            vm.unresolved_violations::DOUBLE
                / GREATEST(vm.total_violations, 1)                             AS persistence_score,
            ou.total_units,
            vm.unresolved_violations,
            -- composite
            (vm.severity_score * {w['severity']})
            + ((vm.total_violations::DOUBLE / ou.total_units)
               * {scale} * {w['density']})
            + ((vm.unique_bbls::DOUBLE / GREATEST(ot.num_properties, 1))
               * {scale} * {w['widespread']})
            + ((vm.unresolved_violations::DOUBLE / GREATEST(vm.total_violations, 1))
               * {scale} * {w['persistence']})
            AS total_harm_score
        FROM viol_metrics vm
        JOIN owner_totals ot USING (owner_id)
        JOIN owner_units  ou USING (owner_id)
        ORDER BY total_harm_score DESC
    """).pl()

    con.close()

    print(f"Scored {len(result)} owners (raw components)")

    # Stage 2: SVI-style composite
    result = _add_svi_composite(result)
    print("SVI composite + Likert assigned")

    return result


# ---------------------------------------------------------------------------
# Stage 2 — SVI-style percentile composite
# ---------------------------------------------------------------------------

def _percentile_rank(series: pl.Series) -> pl.Series:
    """Return percentile rank (0-1) for each value in the series.

    Uses the 'average' tie-breaking method: ties get the average of
    the ranks they span, then divided by n.  Handles n=1 by returning 0.5.
    """
    n = len(series)
    if n <= 1:
        return pl.Series("pctl", [0.5] * n, dtype=pl.Float64)
    ranks = series.rank(method="average")
    return ((ranks - 1) / (n - 1)).alias("pctl")


def _add_svi_composite(df: pl.DataFrame) -> pl.DataFrame:
    """Add SVI composite score and Likert level to the harm-score DataFrame.

    1. Assign each owner to a percentile pool (own jurisdiction if
       n >= MIN_JURISDICTION_SIZE, else 'small_pool').
    2. Percentile-rank each raw component within its pool.
    3. Average percentiles within each SVI theme.
    4. Average theme scores → svi_composite (0-1).
    5. Map composite to Likert level + label.
    """
    # Determine pool assignment
    jur_counts = df.group_by("jurisdiction").len().rename({"len": "jur_n"})
    df = df.join(jur_counts, on="jurisdiction")
    df = df.with_columns(
        pl.when(pl.col("jur_n") >= MIN_JURISDICTION_SIZE)
        .then(pl.col("jurisdiction"))
        .otherwise(pl.lit("__small_pool__"))
        .alias("_pctl_pool"),
    )

    # All raw component columns we need to percentile-rank
    all_components: list[str] = []
    for cols in SVI_THEMES.values():
        all_components.extend(cols)
    all_components = list(dict.fromkeys(all_components))  # dedupe, preserve order

    # Percentile-rank within each pool
    groups: list[pl.DataFrame] = []
    for pool_name, pool_df in df.group_by("_pctl_pool"):
        pctl_cols = []
        for comp in all_components:
            pctl = _percentile_rank(pool_df[comp]).alias(f"{comp}_pctl")
            pctl_cols.append(pctl)
        pool_df = pool_df.with_columns(pctl_cols)
        groups.append(pool_df)

    df = pl.concat(groups)

    # Compute theme scores (average of percentile-ranked components in theme)
    theme_exprs = []
    theme_col_names = []
    for theme, components in SVI_THEMES.items():
        pctl_names = [f"{c}_pctl" for c in components]
        col_name = f"theme_{theme}"
        theme_col_names.append(col_name)
        if len(pctl_names) == 1:
            theme_exprs.append(pl.col(pctl_names[0]).alias(col_name))
        else:
            # Average of the percentile columns
            expr = sum(pl.col(c) for c in pctl_names) / len(pctl_names)
            theme_exprs.append(expr.alias(col_name))

    df = df.with_columns(theme_exprs)

    # SVI composite = average of theme scores
    composite_expr = sum(pl.col(c) for c in theme_col_names) / len(theme_col_names)
    df = df.with_columns(composite_expr.alias("svi_composite"))

    # Likert mapping
    likert_level_expr = pl.lit(5)  # default to highest
    likert_label_expr = pl.lit(LIKERT_SCALE[-1][2])
    likert_color_expr = pl.lit(LIKERT_SCALE[-1][3])

    # Build from last to first so first match wins
    for upper, level, label, color in reversed(LIKERT_SCALE):
        likert_level_expr = (
            pl.when(pl.col("svi_composite") <= upper)
            .then(pl.lit(level))
            .otherwise(likert_level_expr)
        )
        likert_label_expr = (
            pl.when(pl.col("svi_composite") <= upper)
            .then(pl.lit(label))
            .otherwise(likert_label_expr)
        )
        likert_color_expr = (
            pl.when(pl.col("svi_composite") <= upper)
            .then(pl.lit(color))
            .otherwise(likert_color_expr)
        )

    df = df.with_columns(
        likert_level_expr.alias("likert_level"),
        likert_label_expr.alias("likert_label"),
        likert_color_expr.alias("likert_color"),
    )

    # Clean up internal columns
    df = df.drop("_pctl_pool", "jur_n")

    return df.sort("total_harm_score", descending=True)
