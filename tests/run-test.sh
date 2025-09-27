#!/bin/bash

# Ensure script exits on any error
set -e

# Build the worker image
cd ..
docker build -f worker/Dockerfile -t "worker:latest" .
cd tests

# Generate the docker-compose file for testing
./generar-compose-test.sh 3 "YEAR_FILTER"

# Start the containers
docker compose -f test-compose.yaml up -d --build

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to be ready..."
until docker exec rabbit rabbitmq-diagnostics -q ping; do
    sleep 2
done
echo "RabbitMQ is ready!"

# Run the test inside the docker network
docker run --rm \
    --network test-run_temp-net \
    -v $(pwd)/year_filter_test.py:/app/test.py \
    -w /app \
    python:3.9 \
    sh -c "pip install pika && python test.py"

# Clean up
docker compose -f test-compose.yaml down
docker network rm test-run_temp-net 2>/dev/null || true