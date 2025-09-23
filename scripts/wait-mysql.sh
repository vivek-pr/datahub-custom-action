#!/usr/bin/env sh
set -e

HOST="${1:-host}"
PORT="${2:-port}"
USER="${MYSQL_USER:-root}"
PASS="${MYSQL_PASSWORD:-}"

for i in $(seq 1 60); do
  if [ -n "$PASS" ]; then
    if mysqladmin ping -h "$HOST" -P "$PORT" -u"$USER" --password="$PASS" >/dev/null 2>&1; then
      exit 0
    fi
  else
    if mysqladmin ping -h "$HOST" -P "$PORT" -u"$USER" >/dev/null 2>&1; then
      exit 0
    fi
  fi
  sleep 1
done

echo "MySQL not healthy" >&2

exit 1
