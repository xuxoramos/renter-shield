#!/bin/sh
set -e

# --- Simple process supervisor: restart background services if they die ---
supervise() {
    name="$1"; shift
    while true; do
        echo "[supervisor] starting $name"
        "$@" || true
        echo "[supervisor] $name exited, restarting in 2s..."
        sleep 2
    done
}

# Start FastAPI (supervised, background)
supervise uvicorn uvicorn renter_shield.api:app \
    --host 0.0.0.0 --port 8000 \
    --workers 2 \
    --log-level info &

# Start renter Streamlit (supervised, background)
supervise renter-streamlit streamlit run streamlit_renter.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false &

# Start investigator Streamlit (supervised, foreground — keeps container alive)
exec sh -c 'while true; do
    echo "[supervisor] starting investigator-streamlit"
    streamlit run streamlit_investigator.py \
        --server.port 8502 \
        --server.address 0.0.0.0 \
        --server.headless true \
        --server.baseUrlPath investigator \
        --browser.gatherUsageStats false || true
    echo "[supervisor] investigator-streamlit exited, restarting in 2s..."
    sleep 2
done'
