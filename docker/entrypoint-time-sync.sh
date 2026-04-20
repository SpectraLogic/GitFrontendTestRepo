#!/bin/bash
set -e

echo "[time-sync] Syncing time with NTP server..."
if command -v ntpdate >/dev/null 2>&1; then
    ntpdate -u ntp || echo "[time-sync] Warning: NTP sync failed"
else
    echo "[time-sync] ntpdate not installed"
fi

echo "[startup] Executing original entrypoint: $@"
exec "$@"
