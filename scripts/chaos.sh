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
    MATCHING_CONTAINERS=$(docker ps --format "{{.Names}}" | grep "$CONTAINER_PATTERN" || true)

    # Shuffle & pick N
    CONTAINERS_TO_KILL=$(echo "$MATCHING_CONTAINERS" | shuf | head -n "$NUM_TO_KILL")

    for CONTAINER in $CONTAINERS_TO_KILL; do
        # Skip empty entries (can happen if no matches)
        [ -z "$CONTAINER" ] && continue

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing: $CONTAINER"
        docker kill "$CONTAINER" >/dev/null 2>&1 || true
    done

    sleep "$CHAOS_INTERVAL"
done
