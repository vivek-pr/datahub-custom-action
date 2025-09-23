#!/usr/bin/env sh
set -e

HOST="${1:-host}"
PORT="${2:-port}"
USER="${MYSQL_HEALTH_USER:-${MYSQL_USER:-root}}"
PASS="${MYSQL_HEALTH_PASSWORD:-${MYSQL_PASSWORD:-}}"

for i in $(seq 1 60); do
  if mysqladmin ping --protocol=tcp -h "$HOST" -P "$PORT" -u"$USER" ${PASS:+--password="$PASS"} >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "MySQL not healthy" >&2

exit 1
