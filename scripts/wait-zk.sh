#!/usr/bin/env sh
set -e
for i in $(seq 1 60); do
  if printf ruok | nc -w 2 "$1" 2181 | grep -q imok; then exit 0; fi
  if nc -z "$1" 2181; then sleep 2; continue; fi
  sleep 2
done
exit 1
