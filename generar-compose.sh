#!/bin/bash

# Validación de argumentos de entrada
if [ $# -lt 4 ]; then
  echo "Uso: $0 <archivo_salida> <cantidad_trabajadores_filter_by_year> <cantidad_trabajadores_filter_by_hour> <cantidad_trabajadores_filter_by_amount>"
  exit 1
fi

OUTPUT_FILE="$1"
WORKER_COUNT_FILTER_BY_YEAR="$2"
WORKER_COUNT_FILTER_BY_HOUR="$3"
WORKER_COUNT_FILTER_BY_AMOUNT="$4"

cat > "$OUTPUT_FILE" <<EOL
name: tp1
services:
  client:
    container_name: client
    image: client:latest
    entrypoint: /client
    volumes:
      - ./client:/config
      - ./data:/data
    depends_on:
      - rabbit
      - request_handler
    networks:
      - testing_net

  rabbit:
    container_name: rabbit
    image: rabbitmq:3-management
    ports:
      - 5672:5672
      - 15672:15672 # management UI
    networks:
      - testing_net
    environment:
        RABBITMQ_DEFAULT_USER: guest
        RABBITMQ_DEFAULT_PASS: guest

  request_handler:
    container_name: request_handler
    image: request_handler:latest
    entrypoint: /request_handler
    volumes:
      - ./request_handler/config.yaml:/config/config.yaml
    depends_on:
      - rabbit
    networks:
      - testing_net

  response_builder:
    container_name: response_builder
    image: response_builder:latest
    entrypoint: /response_builder
    volumes:
      - ./response_builder/config.yaml:/config/config.yaml
    depends_on:
      - rabbit
    networks:
      - testing_net

EOL

for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_year_worker$i:
    container_name: filter_by_year_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - CLI_WORKER_JOB=YEAR_FILTER
      - CLI_MIDDLEWARE_INPUTQUEUE=transactions
      - CLI_MIDDLEWARE_OUTPUTQUEUEOREXCHANGE=by_year_filter_output

EOL
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_HOUR; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_hour_worker$i:
    container_name: filter_by_hour_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - CLI_WORKER_JOB=HOUR_FILTER
      - CLI_MIDDLEWARE_INPUTQUEUE=by_year_filter_output
      - CLI_MIDDLEWARE_OUTPUTQUEUEOREXCHANGE=by_hour_filter_output
EOL
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_AMOUNT; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_amount_worker$i:
    container_name: filter_by_amount_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - CLI_WORKER_JOB=AMOUNT_FILTER
      - CLI_MIDDLEWARE_INPUTQUEUE=by_hour_filter_output
      - CLI_MIDDLEWARE_OUTPUTQUEUEOREXCHANGE=results
      - CLI_MIDDLEWARE_OUTPUTROUTINGKEY=query1
EOL
done

cat >> "$OUTPUT_FILE" <<EOL
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL

echo "Archivo '$OUTPUT_FILE' creado exitosamente con $WORKER_COUNT_FILTER_BY_YEAR de tipo filter by year y $WORKER_COUNT_FILTER_BY_HOUR de tipo filter by hour y $WORKER_COUNT_FILTER_BY_AMOUNT de tipo filter by amount."
