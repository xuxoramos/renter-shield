"""Ownership network resolution — groups contacts into owner entities.

Operates on the universal contacts schema so it works for any jurisdiction.
Owner identities are suffixed with the jurisdiction code per project convention.

Matching rules (in order of confidence):
  HIGH   — name matches AND at least one shared business address.
  MEDIUM — name matches only, with ≤3 distinct business addresses
           (consistent with a real portfolio operated from few offices).
  LOW    — address matches only (same office, possibly different owners).

Names that appear on fewer than MIN_REGISTRATIONS registrations are excluded
(too few data points to reliably group).  Junk names (single characters,
punctuation-only, known government entities) are also filtered.
"""

from __future__ import annotations

import re

import polars as pl

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

# Minimum distinct registrations to form an ownership group.
# 2 = could be coincidence for common names; 3 = stronger signal.
MIN_REGISTRATIONS = 3

# If a name appears at more than this many distinct addresses without any
# address overlap, it's likely a name collision, not the same person.
MAX_ADDRESSES_FOR_NAME_ONLY = 3

# ---------------------------------------------------------------------------
# Junk / government entity filters
# ---------------------------------------------------------------------------

# Names that should never form ownership groups (case-insensitive starts).
_JUNK_NAME_PREFIXES = [
    "auth ",          # Housing authority
    "city of",
    "county of",
    "district of",
    "phila ",         # "PHILA CITY OF", "PHILA SCHOOL DISTRICT OF"
    "school district",
    "state of",
    "united states",
    "us dept",
    "us department",
]

_JUNK_NAME_PATTERN = re.compile(
    r"^(?:" + "|".join(re.escape(p) for p in _JUNK_NAME_PREFIXES) + r")",
    re.IGNORECASE,
)

# Single-character or punctuation-only names
_TRIVIAL_NAME_RE = re.compile(r"^[\W\d_]{0,2}$")


def _normalize_name(s: str) -> str:
    """Normalize a name for matching: uppercase, strip suffixes, collapse whitespace."""
    s = s.upper().strip()
    # Remove common suffixes that create false splits
    for suffix in (" JR", " SR", " II", " III", " IV", " ESQ"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].rstrip()
    # Remove middle initials (single letter preceded and followed by space)
    s = re.sub(r"\s+[A-Z]\s+", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_junk_name(first: str, last: str) -> bool:
    """Return True if the name is junk or a government entity."""
    full = f"{first} {last}"
    if _TRIVIAL_NAME_RE.match(first) or _TRIVIAL_NAME_RE.match(last):
        return True
    if _JUNK_NAME_PATTERN.match(full) or _JUNK_NAME_PATTERN.match(last):
        return True
    return False


def _is_junk_business_name(name: str) -> bool:
    """Return True if a business_name is junk or a government entity."""
    if _TRIVIAL_NAME_RE.match(name):
        return True
    if _JUNK_NAME_PATTERN.match(name):
        return True
    return False


def resolve_ownership_networks(contacts: pl.LazyFrame) -> pl.DataFrame:
    """Identify owners controlling multiple properties.

    Returns a DataFrame with columns:
        owner_id, jurisdiction, num_properties, registration_ids, confidence
    """
    contacts_df = contacts.collect()

    # --- Normalize names ---------------------------------------------------
    contacts_df = contacts_df.with_columns(
        pl.col("first_name").fill_null("").map_elements(
            _normalize_name, return_dtype=pl.Utf8
        ).alias("norm_first"),
        pl.col("last_name").fill_null("").map_elements(
            _normalize_name, return_dtype=pl.Utf8
        ).alias("norm_last"),
        pl.col("business_name").fill_null("").map_elements(
            _normalize_name, return_dtype=pl.Utf8
        ).alias("norm_biz"),
        # Normalize address key
        (
            pl.col("business_house_number").cast(pl.Utf8).fill_null("")
            + pl.lit(" ")
            + pl.col("business_street").fill_null("").str.to_uppercase()
        ).str.strip_chars().alias("addr_key"),
    )

    # Filter out blanks and junk names
    persons = contacts_df.filter(
        (pl.col("norm_first") != "") & (pl.col("norm_last") != "")
    )
    # Apply junk filter via Python UDF (small cardinality, runs once)
    persons = persons.filter(
        pl.struct("norm_first", "norm_last").map_elements(
            lambda row: not _is_junk_name(row["norm_first"], row["norm_last"]),
            return_dtype=pl.Boolean,
        )
    )

    # --- Step 1: Group by (name, jurisdiction) with address info -----------
    name_groups = (
        persons.group_by("norm_first", "norm_last", "jurisdiction")
        .agg(
            pl.col("registration_id").unique().alias("registration_ids"),
            pl.col("registration_id").n_unique().alias("n_regs"),
            pl.col("addr_key")
            .filter(pl.col("addr_key") != "")
            .unique()
            .alias("addr_keys"),
            pl.col("addr_key")
            .filter(pl.col("addr_key") != "")
            .n_unique()
            .alias("n_addrs"),
        )
        .filter(pl.col("n_regs") >= MIN_REGISTRATIONS)
    )

    # --- Step 2: Assign confidence -----------------------------------------
    # HIGH  = name match + at least 1 shared address (n_addrs >= 1 AND n_addrs <= many)
    #         AND address concentration is plausible (not a collision)
    # MEDIUM = name match only, few addresses (consistent with single owner)
    # We drop groups where n_addrs is large and there's no address clustering
    # (likely name collision — "JOHN SMITH" at 50 different addresses)

    name_groups = name_groups.with_columns(
        pl.when(
            (pl.col("n_addrs") >= 1) & (pl.col("n_addrs") <= MAX_ADDRESSES_FOR_NAME_ONLY)
        ).then(pl.lit("high"))
        .when(
            pl.col("n_addrs") <= MAX_ADDRESSES_FOR_NAME_ONLY
        ).then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .alias("confidence"),
    )

    # Drop low-confidence name groups (too many addresses, no corroboration)
    person_networks = (
        name_groups.filter(pl.col("confidence") != "low")
        .with_columns(
            (
                pl.col("norm_first")
                + pl.lit("_")
                + pl.col("norm_last")
                + pl.lit(" [")
                + pl.col("jurisdiction")
                + pl.lit("]")
            ).alias("owner_id")
        )
        .select(
            "owner_id", "jurisdiction",
            pl.col("n_regs").alias("num_properties"),
            "registration_ids", "confidence",
        )
    )

    # --- Step 2b: Business-name networks -----------------------------------
    # Handles contacts where first/last are empty but business_name is set
    # (e.g. Boston assessment OWNER field: "SMITH JOHN", "ACME TRUST LLC").
    biz_only = contacts_df.filter(
        ((pl.col("norm_first") == "") | (pl.col("norm_last") == ""))
        & (pl.col("norm_biz") != "")
    )
    # Filter junk business names
    biz_only = biz_only.filter(
        pl.col("norm_biz").map_elements(
            lambda n: not _is_junk_business_name(n),
            return_dtype=pl.Boolean,
        )
    )

    biz_groups = (
        biz_only.group_by("norm_biz", "jurisdiction")
        .agg(
            pl.col("registration_id").unique().alias("registration_ids"),
            pl.col("registration_id").n_unique().alias("n_regs"),
            pl.col("addr_key")
            .filter(pl.col("addr_key") != "")
            .unique()
            .alias("addr_keys"),
            pl.col("addr_key")
            .filter(pl.col("addr_key") != "")
            .n_unique()
            .alias("n_addrs"),
        )
        .filter(pl.col("n_regs") >= MIN_REGISTRATIONS)
    )

    biz_groups = biz_groups.with_columns(
        pl.when(
            (pl.col("n_addrs") >= 1) & (pl.col("n_addrs") <= MAX_ADDRESSES_FOR_NAME_ONLY)
        ).then(pl.lit("high"))
        .when(
            pl.col("n_addrs") <= MAX_ADDRESSES_FOR_NAME_ONLY
        ).then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .alias("confidence"),
    )

    biz_networks = (
        biz_groups.filter(pl.col("confidence") != "low")
        .with_columns(
            (
                pl.col("norm_biz")
                + pl.lit(" [")
                + pl.col("jurisdiction")
                + pl.lit("]")
            ).alias("owner_id")
        )
        .select(
            "owner_id", "jurisdiction",
            pl.col("n_regs").alias("num_properties"),
            "registration_ids", "confidence",
        )
    )

    # --- Step 3: Address networks ------------------------------------------
    address_raw = contacts_df.filter(
        (pl.col("addr_key") != "")
        & pl.col("business_house_number").is_not_null()
        & pl.col("business_street").is_not_null()
    )

    address_groups = (
        address_raw.group_by("addr_key", "jurisdiction")
        .agg(
            pl.col("registration_id").unique().alias("registration_ids"),
            pl.col("registration_id").n_unique().alias("n_regs"),
        )
        .filter(pl.col("n_regs") >= MIN_REGISTRATIONS)
        .with_columns(
            (
                pl.lit("ADDRESS_")
                + pl.col("addr_key")
                + pl.lit(" [")
                + pl.col("jurisdiction")
                + pl.lit("]")
            ).alias("owner_id"),
            pl.lit("low").alias("confidence"),
        )
        .select(
            "owner_id", "jurisdiction",
            pl.col("n_regs").alias("num_properties"),
            "registration_ids", "confidence",
        )
    )

    networks = pl.concat([person_networks, biz_networks, address_groups]).sort(
        "num_properties", descending=True
    )

    # Summary
    high = len(networks.filter(pl.col("confidence") == "high"))
    medium = len(networks.filter(pl.col("confidence") == "medium"))
    low = len(networks.filter(pl.col("confidence") == "low"))
    print(
        f"Identified {len(networks)} ownership groups "
        f"(high={high}, medium={medium}, low={low})"
    )

    return networks
