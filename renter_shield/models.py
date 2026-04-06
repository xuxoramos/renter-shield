"""Column schemas for the normalized intermediate DataFrames.

Every jurisdiction adapter must produce DataFrames whose columns are a superset
of the schemas defined here. Extra columns are allowed but ignored downstream.
"""

# Violations — one row per violation event
VIOLATIONS_SCHEMA = {
    "violation_id":   "Utf8",   # unique within the jurisdiction
    "bbl":            "Utf8",   # canonical parcel identifier
    "severity_tier":  "Int8",   # 1-4 mapped from local classification
    "status":         "Utf8",   # "open" | "closed"
    "inspection_date":"Date",   # when the violation was recorded
    "jurisdiction":   "Utf8",   # e.g. "nyc"
}

# Properties — one row per registered property
PROPERTIES_SCHEMA = {
    "bbl":              "Utf8",
    "registration_id":  "Utf8",
    "units_residential":"Float64",
    "year_built":       "Utf8",
    "address":          "Utf8",
    "jurisdiction":     "Utf8",
}

# Contacts — one row per owner/agent contact linked to a registration
CONTACTS_SCHEMA = {
    "registration_id":  "Utf8",
    "first_name":       "Utf8",
    "last_name":        "Utf8",
    "business_name":    "Utf8",
    "business_house_number": "Utf8",
    "business_street":  "Utf8",
    "jurisdiction":     "Utf8",
}
