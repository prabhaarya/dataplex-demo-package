#!/usr/bin/env bash
set -euo pipefail

# Determine script directory for execution context
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set up gcloud path and env vars for Context Aware Access
if ! command -v gcloud &> /dev/null; then
    if [ -d "/Users/prabhaarya/Downloads/google-cloud-sdk/bin" ]; then
        export PATH="/Users/prabhaarya/Downloads/google-cloud-sdk/bin:$PATH"
    elif [ -d "$HOME/google-cloud-sdk/bin" ]; then
        export PATH="$HOME/google-cloud-sdk/bin:$PATH"
    fi
fi
export CLOUDSDK_CONTEXT_AWARE_USE_CERTIFICATE=true

# Activate virtual environment
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

ACTIVE_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "prabha-test")
ACTIVE_PROJECT=${ACTIVE_PROJECT:-"prabha-test"}

echo ">>> Setting ASPECT_TYPES_PROJECT"
export ASPECT_TYPES_PROJECT="${ASPECT_TYPES_PROJECT:-$ACTIVE_PROJECT}" 

echo ">>> Setting DATA_PRODUCTS_PROJECT"
export DATA_PRODUCTS_PROJECT="${DATA_PRODUCTS_PROJECT:-$ACTIVE_PROJECT}" 

export DBT_PROJECT_ID="${DBT_PROJECT_ID:-$ACTIVE_PROJECT}"

echo "

>>> Running all dbt models

"
#dbt run --select sms_delivery_receipts_demo
dbt run --profiles-dir . 

echo "

>>> Syncing aspects to changed models's table and columns

"
python scripts/sync_table_aspects.py --target-dir target #--verbose

echo "

>>> Syncing Data Products to Knowledge Catalog

"
python scripts/sync_data_products.py

echo ">>> Done."
