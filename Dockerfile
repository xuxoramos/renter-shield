FROM python:3.12-slim AS base

WORKDIR /app

# System deps (none needed beyond Python, but keep layer for future)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e "." streamlit

# Copy source
COPY renter_shield/ renter_shield/
COPY streamlit_renter.py streamlit_investigator.py ./

# Output dir will be mounted as a volume at runtime
RUN mkdir -p output

# Expose services: API, renter Streamlit, investigator Streamlit
EXPOSE 8000 8501 8502

# Default: run via supervisord-like entrypoint (see entrypoint.sh)
COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV LI_OUTPUT_DIR=/app/output \
    LI_API_KEYS=changeme

ENTRYPOINT ["/entrypoint.sh"]
