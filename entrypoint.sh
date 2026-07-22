#!/usr/bin/env bash
set -euo pipefail

MUNGE_KEY_SOURCE="${MUNGE_KEY_SOURCE:-/mnt/munge/munge.key}"

install -d -m 0700 -o munge -g munge /etc/munge /var/log/munge
install -d -m 0755 -o munge -g munge /run/munge
install -m 0400 -o munge -g munge "$MUNGE_KEY_SOURCE" /etc/munge/munge.key
runuser -u munge -- munged --num-threads 2 -F &

for _ in $(seq 100); do
    [ -S /run/munge/munge.socket.2 ] && break
    sleep 0.1
done
munge -n >/dev/null

exec uv run --locked sync_groups.py
