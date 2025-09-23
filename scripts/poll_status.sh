#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <dataset-urn> [timeout-seconds]" >&2
  exit 1
fi

DATASET_URN="$1"
TIMEOUT="${2:-600}"
SLEEP_INTERVAL=10

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

END_TIME=$(( $(date +%s) + TIMEOUT ))

while [ $(date +%s) -lt $END_TIME ]; do
  STATUS=$(${COMPOSE_CMD} exec -T \
    -e DATASET_URN="$DATASET_URN" \
    datahub-actions python - <<'PY'
import os
from action.datahub_client import DataHubClient

dataset_urn = os.environ['DATASET_URN']
client = DataHubClient()
dataset = client.get_dataset(dataset_urn)
tags = client._extract_tag_urns(dataset.get("globalTags"))
status_tag = next((tag for tag in tags if tag.startswith("urn:li:tag:tokenize/status:")), None)
if status_tag:
    print(status_tag.rsplit(":", 1)[-1])
PY
  )
  STATUS=$(echo "$STATUS" | tr -d '\r')
  if [ -n "$STATUS" ]; then
    echo "Current status: $STATUS"
    if [ "$STATUS" = "SUCCESS" ]; then
      exit 0
    elif [ "$STATUS" = "FAILED" ]; then
      echo "Tokenization failed" >&2
      exit 2
    fi
  else
    echo "Waiting for status..."
  fi
  sleep $SLEEP_INTERVAL
done

echo "Timed out waiting for status update" >&2
exit 3
