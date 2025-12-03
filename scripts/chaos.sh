#!/bin/bash
set -e

CHAOS_INTERVAL="${1:-30}"
CONTAINER_PATTERN="${2:-}"
NUM_TO_KILL="${3:-1}"

echo -e "Starting chaos tool in 5 seconds..."
echo ""
echo -e "Interval: ${CHAOS_INTERVAL}"
echo -e "Target pattern: ${CONTAINER_PATTERN}"
echo -e "Containers to kill per interval: ${NUM_TO_KILL}"
sleep 5

while true; do
    CONTAINERS_TO_KILL=$(docker ps --format "{{.Names}}" | grep "$CONTAINER_PATTERN" | shuf | head -n "$NUM_TO_KILL")

    for CONTAINER in $CONTAINERS_TO_KILL; do
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing: $CONTAINER"
        docker kill "$CONTAINER" >/dev/null 2>&1
    done

    sleep "$CHAOS_INTERVAL"
done
