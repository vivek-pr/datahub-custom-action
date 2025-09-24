#!/usr/bin/env bash
# Wait for one or more docker compose services to reach a healthy state.
# Usage: wait-for-health.sh "docker compose" service1 [service2 ...]
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <compose-command> <service> [service...]" >&2
  exit 1
fi

COMPOSE_CMD="$1"
shift
SERVICES=("$@")
WAIT_TIMEOUT="${WAIT_TIMEOUT:-600}"
WAIT_INTERVAL="${WAIT_INTERVAL:-5}"

wait_for_service() {
  local service="$1"
  local deadline=$((SECONDS + WAIT_TIMEOUT))

  echo "Waiting for service '$service' to become healthy..."
  while true; do
    local container_id
    container_id=$(${COMPOSE_CMD} ps -q "$service" 2>/dev/null || true)
    if [ -z "$container_id" ]; then
      if (( SECONDS >= deadline )); then
        echo "Timed out waiting for container id for service '$service'." >&2
        ${COMPOSE_CMD} ps >&2 || true
        return 1
      fi
      sleep "$WAIT_INTERVAL"
      continue
    fi

    local status
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)

    case "$status" in
      healthy|running)
        echo "Service '$service' is healthy ($status)."
        return 0
        ;;
      unhealthy|exited|dead)
        echo "Service '$service' reported status '$status'." >&2
        ${COMPOSE_CMD} logs "$service" >&2 || true
        return 1
        ;;
      "")
        # Container might be starting up; fall through to retry.
        ;;
      *)
        echo "Service '$service' current status: $status" >&2
        ;;
    esac

    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for service '$service' to become healthy." >&2
      ${COMPOSE_CMD} logs "$service" >&2 || true
      return 1
    fi

    sleep "$WAIT_INTERVAL"
  done
}

for svc in "${SERVICES[@]}"; do
  if ! wait_for_service "$svc"; then
    exit 1
  fi
done
