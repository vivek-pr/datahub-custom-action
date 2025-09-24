#!/usr/bin/env bash
set -euo pipefail

DATASET_NAME=${DATASET_NAME:-customers}
PLATFORM=${PLATFORM:-postgres}
TIMEOUT=${TIMEOUT:-600}
ACTION_PORT=${ACTION_PORT:-8091}

DATASET_URN=$(python3 scripts/find_dataset_urn.py "$DATASET_NAME" "$PLATFORM" | head -n 1 | cut -f1)
if [ -z "$DATASET_URN" ]; then
  echo "Unable to resolve dataset URN" >&2
  exit 1
fi

echo "Resolved dataset URN: $DATASET_URN"

./scripts/add_tag.sh "$DATASET_URN"
./scripts/poll_status.sh "$DATASET_URN" "$TIMEOUT"

# Trigger via API as well
API_RESPONSE=$(curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"dataset\": \"$DATASET_URN\"}" \
  http://localhost:${ACTION_PORT}/trigger)
echo "API trigger response: $API_RESPONSE"

./scripts/poll_status.sh "$DATASET_URN" "$TIMEOUT"
