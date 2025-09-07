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
  middleware:
    container_name: middleware
    image: middleware:latest
    entrypoint: /middleware
    volumes:
      - ./middleware:/config
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
      - ./worker:/config
    environment:
      - CLI_ID=$i
    networks:
      - testing_net
    depends_on:
      - middleware

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
