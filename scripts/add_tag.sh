#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <dataset-urn> [tag-urn]" >&2
  exit 1
fi

DATASET_URN="$1"
TAG_URN="${2:-urn:li:tag:tokenize/run}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

${COMPOSE_CMD} exec -T \
  -e DATASET_URN="$DATASET_URN" \
  -e TAG_URN="$TAG_URN" \
  datahub-actions python - <<'PY'
import os
from action.datahub_client import DataHubClient

dataset_urn = os.environ['DATASET_URN']
tag_urn = os.environ['TAG_URN']
client = DataHubClient()
client.update_dataset_tags(dataset_urn, add=[tag_urn])
print(f"Applied {tag_urn} to {dataset_urn}")
PY
