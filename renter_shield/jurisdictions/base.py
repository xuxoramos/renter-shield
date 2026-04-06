"""Abstract base class that every jurisdiction adapter must implement."""

from __future__ import annotations

import abc
from pathlib import Path

import polars as pl


class JurisdictionAdapter(abc.ABC):
    """Contract for jurisdiction-specific data ingestion and normalization.

    Subclasses handle downloading, loading, and transforming local data into
    the universal schema defined in ``models.py``.
    """

    # Short lowercase code used in owner IDs, e.g. "nyc", "chicago"
    jurisdiction_code: str = ""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    # ------------------------------------------------------------------
    # Optional: download raw data (CSV / API) into data_dir
    # ------------------------------------------------------------------
    def download(self) -> None:
        """Download raw data files. Override if the jurisdiction supports it."""
        raise NotImplementedError(
            f"{self.jurisdiction_code}: automatic download not implemented"
        )

    # ------------------------------------------------------------------
    # Required: load + normalize into universal schema
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def load_violations(self) -> pl.LazyFrame:
        """Return violations conforming to ``models.VIOLATIONS_SCHEMA``."""

    @abc.abstractmethod
    def load_properties(self) -> pl.LazyFrame:
        """Return properties conforming to ``models.PROPERTIES_SCHEMA``."""

    @abc.abstractmethod
    def load_contacts(self) -> pl.LazyFrame:
        """Return contacts conforming to ``models.CONTACTS_SCHEMA``."""
