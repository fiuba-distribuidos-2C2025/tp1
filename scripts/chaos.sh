#!/bin/bash
set -e

INTERVAL="${1:-30s}"
CONTAINER_FILTER="${2:-}"

echo -e "Starting chaos tool in 5 seconds..."
echo ""
echo -e "Interval: ${INTERVAL}"
echo -e "Target containers: ${CONTAINER_FILTER:-all containers}"
sleep 5

docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock gaiaadm/pumba \
    --interval="$INTERVAL" --random -l "info" kill --signal="SIGKILL" "$CONTAINER_FILTER"
