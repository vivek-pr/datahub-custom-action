#!/usr/bin/env bash
set -euo pipefail

IFS=$' \t\n'
URIS=("$@")
if [ ${#URIS[@]} -eq 0 ] && [ -n "${WAIT_FOR_URIS:-}" ]; then
  # shellcheck disable=SC2206
  URIS=(${WAIT_FOR_URIS})
fi

if [ ${#URIS[@]} -eq 0 ]; then
  echo "wait-for-uris: no URIs provided" >&2
  exit 0
fi

check_http() {
  local url=$1
  local attempt=0
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    echo "wait-for-uris: waiting for $url (attempt $attempt)" >&2
    sleep 2
  done
}

check_tcp() {
  local target=$1
  local host=${target%:*}
  local port=${target##*:}
  if [[ -z $host || -z $port || $host == $port ]]; then
    echo "wait-for-uris: invalid tcp target '$target'" >&2
    return 1
  fi
  local attempt=0
  while true; do
    if (echo > /dev/tcp/$host/$port) >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    echo "wait-for-uris: waiting for tcp://$target (attempt $attempt)" >&2
    sleep 2
  done
}

for raw in "${URIS[@]}"; do
  uri=$(echo "$raw" | xargs)
  [ -z "$uri" ] && continue
  case "$uri" in
    http://*|https://*)
      check_http "$uri"
      ;;
    tcp://*)
      check_tcp "${uri#tcp://}"
      ;;
    *)
      echo "wait-for-uris: unsupported URI '$uri'" >&2
      exit 1
      ;;
  esac
done
