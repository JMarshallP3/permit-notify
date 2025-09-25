#!/bin/bash
set -e

echo "Running database migrations..."
# First try to upgrade to the merge revision
python -m alembic upgrade 011_merge_heads || echo "Merge migration may already be applied"

echo "Starting application..."
uvicorn app.main:app --host 0.0.0.0 --port 8000
