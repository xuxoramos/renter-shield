FROM python:3.12-slim AS base

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e "."

# Copy source
COPY renter_shield/ renter_shield/

# Output dir will be mounted as a volume at runtime
RUN mkdir -p output

# Expose FastAPI only
EXPOSE 8000

# Default: run via entrypoint
COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV LI_OUTPUT_DIR=/app/output \
    LI_API_KEYS=changeme

# Health check — single FastAPI service
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf http://127.0.0.1:8000/health > /dev/null || exit 1

ENTRYPOINT ["/entrypoint.sh"]
