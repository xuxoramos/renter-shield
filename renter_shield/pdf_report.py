"""Generate a printable PDF property violation report.

Uses fpdf2 (pure Python, no system dependencies).
"""

from __future__ import annotations

import re
from io import BytesIO

from fpdf import FPDF

# Strip emoji characters that can't render in standard PDF fonts
_EMOJI_RE = re.compile(
    "[\U0001f300-\U0001f9ff\U00002600-\U000027bf\U0000fe00-\U0000fe0f"
    "\U0000200d\U00002702-\U000027b0\U0000231a-\U0000231b"
    "\U000023e9-\U000023f3\U000023f8-\U000023fa]+",
)


def _strip_emoji(text: str) -> str:
    return _EMOJI_RE.sub("", text).strip()


def generate_property_report(
    *,
    address: str,
    jurisdiction: str,
    rating_label: str,
    units: str,
    year_built: str,
    total_violations: int,
    critical: int,
    open_violations: int,
    open_pct: float,
    owner_name: str | None = None,
    owner_rating: str | None = None,
    owner_properties: int | None = None,
    owner_total_violations: int | None = None,
    violations: list[dict] | None = None,
) -> bytes:
    """Return PDF bytes for a property violation report.

    Parameters
    ----------
    violations : list of dicts with keys "date", "severity", "status".
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # --- Header ---
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 10, "Renter Shield", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, "Property Violation Report", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(
        0, 5,
        "Open-source housing transparency tool",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.ln(3)

    # Disclaimer
    pdf.set_fill_color(255, 251, 235)
    pdf.set_draw_color(252, 211, 77)
    pdf.set_text_color(146, 64, 14)
    pdf.set_font("Helvetica", "", 7)
    pdf.multi_cell(
        0, 4,
        "DISCLAIMER: Scores are derived from publicly available government "
        "records and algorithmic analysis. They do not constitute legal "
        "findings, may not reflect current conditions, and may contain errors "
        "due to name-based ownership resolution. Independent verification is "
        "required before any legal, administrative, or public action.",
        border=1, fill=True,
    )
    pdf.ln(5)

    # --- Property info ---
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, address, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 10)
    _info_row(pdf, "Jurisdiction", jurisdiction)
    _info_row(pdf, "Rating", _strip_emoji(rating_label))
    _info_row(pdf, "Residential Units", units)
    _info_row(pdf, "Year Built", year_built)
    pdf.ln(3)

    # --- Violation summary ---
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Violation Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    _info_row(pdf, "Total Violations", f"{total_violations:,}")
    _info_row(pdf, "Critical (Tier 1)", f"{critical:,}")
    _info_row(pdf, "Open / Unresolved", f"{open_violations:,} ({open_pct:.0%})")
    pdf.ln(3)

    # --- Owner info (if available) ---
    if owner_name:
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 7, "Landlord Information", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        _info_row(pdf, "Name", owner_name)
        if owner_rating:
            _info_row(pdf, "Landlord Rating", _strip_emoji(owner_rating))
        if owner_properties is not None:
            _info_row(pdf, "Properties Managed", f"{owner_properties:,}")
        if owner_total_violations is not None:
            _info_row(
                pdf, "Total Violations (all properties)",
                f"{owner_total_violations:,}",
            )
        pdf.ln(3)

    # --- Violation table ---
    if violations:
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 7, "Violation History", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # Table header
        col_w = [30, 55, 55, 50]
        headers = ["Date", "Severity", "Status", "Violation ID"]
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(240, 240, 240)
        for i, h in enumerate(headers):
            pdf.cell(col_w[i], 6, h, border=1, fill=True)
        pdf.ln()

        # Table rows (up to 100)
        pdf.set_font("Helvetica", "", 8)
        for v in violations[:100]:
            # Check if we need a new page
            if pdf.get_y() > 260:
                pdf.add_page()
                pdf.set_font("Helvetica", "B", 8)
                for i, h in enumerate(headers):
                    pdf.cell(col_w[i], 6, h, border=1, fill=True)
                pdf.ln()
                pdf.set_font("Helvetica", "", 8)

            pdf.cell(col_w[0], 5, str(v.get("date", "")), border=1)
            pdf.cell(col_w[1], 5, str(v.get("severity", "")), border=1)
            pdf.cell(col_w[2], 5, str(v.get("status", "")), border=1)
            pdf.cell(col_w[3], 5, str(v.get("violation_id", ""))[:20], border=1)
            pdf.ln()

        if len(violations) > 100:
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(
                0, 6,
                f"... and {len(violations) - 100:,} more violations (not shown)",
            )
            pdf.ln()

    # --- Footer ---
    pdf.ln(8)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(100, 100, 100)
    pdf.multi_cell(
        0, 3.5,
        "Generated by Renter Shield (https://github.com/xuxoramos/renter-shield). "
        "Data sourced from government open-data portals. "
        "This report is not a legal document. Consult a tenant rights attorney "
        "for legal guidance.",
    )

    buf = BytesIO()
    pdf.output(buf)
    return buf.getvalue()


def _info_row(pdf: FPDF, label: str, value: str) -> None:
    """Render a label: value row."""
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(55, 5, f"{label}:", new_x="END")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, value, new_x="LMARGIN", new_y="NEXT")
