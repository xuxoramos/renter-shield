#!/bin/sh
set -e

# Start FastAPI in the background
uvicorn renter_shield.api:app \
    --host 0.0.0.0 --port 8000 \
    --workers 2 \
    --log-level info &

# Start renter Streamlit in the background (port 8501)
streamlit run streamlit_renter.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false &

# Start investigator Streamlit in the foreground (port 8502)
exec streamlit run streamlit_investigator.py \
    --server.port 8502 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false
