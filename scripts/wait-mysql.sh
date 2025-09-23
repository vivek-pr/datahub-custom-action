#!/usr/bin/env sh
set -eu

HOST="${1:-${MYSQL_HOST:-localhost}}"
PORT="${2:-${MYSQL_PORT:-3306}}"
USER="${MYSQL_HEALTH_USER:-${MYSQL_USER:-root}}"
PASS="${MYSQL_HEALTH_PASSWORD:-${MYSQL_PASSWORD:-}}"

echo "Waiting for MySQL at $HOST:$PORT (user=$USER)"

for i in $(seq 1 60); do
  if [ -n "$PASS" ]; then
    if mysqladmin ping \
        --protocol=tcp \
        -h"$HOST" -P"$PORT" \
        -u"$USER" -p"$PASS" \
        >/dev/null 2>&1; then
      echo "MySQL is healthy"
      exit 0
    fi
  else
    if mysqladmin ping \
        --protocol=tcp \
        -h"$HOST" -P"$PORT" \
        -u"$USER" \
        >/dev/null 2>&1; then
      echo "MySQL is healthy"
      exit 0
    fi
  fi
  sleep 1
done

echo "MySQL not healthy after 60s" >&2
exit 1
