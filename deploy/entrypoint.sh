#!/bin/sh
set -e

# Single process: FastAPI serves everything (API + web UI).
# Two workers fit comfortably on a 4 GB VPS (~150-200 MB each).
exec uvicorn renter_shield.api:app \
    --host 0.0.0.0 --port 8000 \
    --workers 2 \
    --log-level info
