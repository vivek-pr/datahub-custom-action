#!/usr/bin/env sh
set -euo pipefail

timeout_secs="${WAIT_TIMEOUT_SECONDS:-240}"
start_ts="$(date +%s)"

# Split on whitespace
IFS=' ' read -r -a URIS <<< "${WAIT_FOR_URIS:-}"

is_up() {
  uri="$1"
  case "$uri" in
    http://*|https://*)
      # Require at least host part
      if ! echo "$uri" | grep -Eq '^https?://[^/]+(:[0-9]+)?(/.*)?$'; then
        echo "⚠️ Skipping invalid http(s) URI: '$uri'"
        return 0
      fi
      curl -fsS --max-time 5 "$uri" >/dev/null 2>&1
      ;;
    tcp://*)
      hostport="${uri#tcp://}"
      host="${hostport%:*}"
      port="${hostport##*:}"
      if [ -z "$host" ] || [ -z "$port" ]; then
        echo "⚠️ Skipping invalid tcp URI: '$uri'"
        return 0
      fi
      # Use bash /dev/tcp if available, else nc
      (echo > /dev/tcp/"$host"/"$port") >/dev/null 2>&1 2>/dev/null \
        || nc -z -w 3 "$host" "$port" >/dev/null 2>&1
      ;;
    *)
      echo "⚠️ Unknown/unsupported URI scheme: '$uri' (skipping)"
      return 0
      ;;
  esac
}

filtered=""
for uri in "${URIS[@]}"; do
  [ -n "$uri" ] || continue
  filtered="$filtered $uri"
done

echo "Waiting on dependencies:${filtered:- (none)}"

for uri in $filtered; do
  until is_up "$uri"; do
    now="$(date +%s)"
    elapsed=$(( now - start_ts ))
    if [ "$elapsed" -ge "$timeout_secs" ]; then
      echo "❌ Timeout after ${timeout_secs}s waiting on dependencies: [$filtered]" >&2
      exit 1
    fi
    sleep 2
  done
  echo "✅ $uri is up"
done

echo "🎉 All dependencies are up."
