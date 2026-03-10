#!/bin/sh
set -e

SEEDER_PID=""

cleanup() {
  echo "[init] Shutting down..."
  [ -n "$SEEDER_PID" ] && kill "$SEEDER_PID" 2>/dev/null || true
  exit 0
}

trap cleanup TERM INT

# Phase 1 (root): fix volume ownership
echo "[init] Fixing permissions..."
mkdir -p /workspace /data /local
chown 1000:1000 /workspace /data /local

# Phase 2 (1000:1000): background DB credential seeder
echo "[init] Starting credential seeder in background..."
su-exec 1000:1000 python src/seed_credentials.py &
SEEDER_PID=$!

# Phase 3 (1000:1000): run mock K8s API
echo "[init] Starting Mock K8s API as uid 1000..."
exec su-exec 1000:1000 python src/main.py
