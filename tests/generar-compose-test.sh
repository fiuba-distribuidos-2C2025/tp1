#!/bin/bash

# Validación de argumentos de entrada
if [ $# -lt 2 ]; then
  echo "Uso: $0 <cantidad_trabajadores> <tipo_trabajadores>"
  exit 1
fi

WORKER_COUNT="$1"
WORKER_TYPE="$2"

cat > "test-compose.yaml" <<EOL
name: test-run
services:
  rabbit:
    container_name: rabbit
    image: rabbitmq:3-management
    ports:
      - 5672:5672
      - 15672:15672 # management UI
    networks:
      - temp-net
    environment:
        RABBITMQ_DEFAULT_USER: guest
        RABBITMQ_DEFAULT_PASS: guest

EOL

for ((i=1; i<=WORKER_COUNT; i++)); do
cat >> "test-compose.yaml" <<EOL
  worker$i:
    container_name: worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ../worker/config.yaml:/config.yaml
    networks:
      - temp-net
    depends_on:
      - rabbit
    environment:
      - CLI_WORKER_JOB=$WORKER_TYPE
      - CLI_MIDDLEWARE_INPUTQUEUE=test-input
      - CLI_MIDDLEWARE_OUTPUTQUEUE=test-output

EOL
done


cat >> "test-compose.yaml" <<EOL
networks:
  temp-net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL

echo "Archivo 'test-compose.yaml' creado exitosamente con $WORKER_COUNT trabajadores de tipo $WORKER_TYPE."
