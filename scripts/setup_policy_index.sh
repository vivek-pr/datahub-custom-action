#!/usr/bin/env sh
set -euo pipefail

HOST="${ELASTICSEARCH_HOST:-elasticsearch}"
PORT="${ELASTICSEARCH_PORT:-9200}"
INDEX="${POLICY_INDEX_NAME:-datahubpolicyindex_v2}"

BASE_URL="http://${HOST}:${PORT}"

if curl -sf "${BASE_URL}/${INDEX}" >/dev/null 2>&1; then
    echo "Index ${INDEX} already exists"
    exit 0
fi

echo "Creating index ${INDEX}"
curl -sf -X PUT "${BASE_URL}/${INDEX}" \
    -H 'Content-Type: application/json' \
    -d '{"settings":{"index":{"number_of_shards":1,"number_of_replicas":0}}}'

echo "Created index ${INDEX}"
