#!/usr/bin/env sh
set -e
HOST="${1:?host}"; PORT="${2:?port}"
for i in $(seq 1 60); do
  if mysqladmin ping -h "$HOST" -P "$PORT" -uroot -prootpass >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done
echo "MySQL not healthy"
exit 1
