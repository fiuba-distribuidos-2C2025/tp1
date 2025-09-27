#!/bin/bash

# Validación de argumentos de entrada
if [ $# -lt 2 ]; then
  echo "Uso: $0 <archivo_salida> <cantidad_trabajadores>"
  exit 1
fi

OUTPUT_FILE="$1"
WORKER_COUNT="$2"

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
      - ./request_handler:/config
    networks:
      - testing_net

EOL

# Agregacion dinamica de trabajadores
for ((i=1; i<=WORKER_COUNT; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  worker$i:
    container_name: worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit

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

echo "Archivo '$OUTPUT_FILE' creado exitosamente con $WORKER_COUNT trabajadores."
