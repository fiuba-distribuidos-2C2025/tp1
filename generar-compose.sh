#!/bin/bash

# Validación de argumentos de entrada
if [ $# -lt 7 ]; then
  echo "Uso: $0 <archivo_salida> <cantidad_trabajadores_filter_by_year> <cantidad_trabajadores_filter_by_hour> <cantidad_trabajadores_filter_by_amount> <cantidad_trabajadores_filter_by_year_items> <cantidad_trabajadores_grouper_by_year_month>"
  exit 1
fi

OUTPUT_FILE="$1"
REQUEST_CONTROLLER_COUNT=1
WORKER_COUNT_FILTER_BY_YEAR="$2"
WORKER_COUNT_FILTER_BY_HOUR="$3"
WORKER_COUNT_FILTER_BY_AMOUNT="$4"
WORKER_COUNT_GROUPER_BY_YEAR_MONTH="$5"
WORKER_COUNT_GROUPER_BY_SEMESTER="$6"

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
    healthcheck:
        test: ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
        interval: 5s
        timeout: 5s
        retries: 5
        start_period: 5s

  request_handler:
    container_name: request_handler
    image: request_handler:latest
    entrypoint: /request_handler
    volumes:
      - ./request_handler/config.yaml:/config/config.yaml
    depends_on:
        rabbit:
            condition: service_healthy
    networks:
      - testing_net
    environment:
      - REQUEST_MIDDLEWARE_RECEIVERS_COUNT=$WORKER_COUNT_FILTER_BY_YEAR

  response_builder:
    container_name: response_builder
    image: response_builder:latest
    entrypoint: /response_builder
    volumes:
      - ./response_builder/config.yaml:/config/config.yaml
    environment:
      - RESPONSE_MIDDLEWARE_RECEIVERS_COUNT=$WORKER_COUNT_FILTER_BY_AMOUNT
    depends_on:
        rabbit:
            condition: service_healthy
    networks:
      - testing_net

EOL

# ==============================================================================
# First Query
# ==============================================================================

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
      - WORKER_JOB=YEAR_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_2024_2025_q1,transactions_2024_2025_q2
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_FILTER_BY_HOUR,$WORKER_COUNT_GROUPER_BY_STORE_USER
      - WORKER_ID=$i


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
      - WORKER_JOB=HOUR_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_2024_2025_q1
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_filtered_by_hour_q1,transactions_filtered_by_hour_q2
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_FILTER_BY_AMOUNT,$WORKER_COUNT_GROUPER_BY_SEMESTER
      - WORKER_ID=$i

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
      - WORKER_JOB=AMOUNT_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_filtered_by_hour_q1
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_1
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_HOUR
      - WORKER_MIDDLEWARE_RECEIVERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_ID=$i

EOL
done

# ==============================================================================
# Second Query
# ==============================================================================


WORKER_COUNT_FILTER_BY_YEAR_ITEMS=$WORKER_COUNT_FILTER_BY_YEAR
WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY=1
WORKER_COUNT_JOINER_BY_ITEM_ID=$WORKER_COUNT_FILTER_BY_YEAR_ITEMS

for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR_ITEMS; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_year_items_worker$i:
    container_name: filter_by_year_items_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=YEAR_FILTER_ITEMS
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_items_2024_2025
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_GROUPER_BY_YEAR_MONTH
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_YEAR_MONTH; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_year_month_worker$i:
    container_name: grouper_by_year_moth_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=GROUPER_BY_YEAR_MONTH
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_items_2024_2025
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=year_month_grouped_items
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR_ITEMS
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  aggregator_by_profit_quantity$i:
    container_name: aggregator_by_profit_quantity$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=AGGREGATOR_BY_PROFIT_QUANTITY
      - WORKER_MIDDLEWARE_INPUTQUEUE=year_month_grouped_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=max_quantity_profit_items
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR_ITEMS
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_ITEM_ID
      - WORKER_ID=$i

EOL
done

# TODO ADD JOINERS BY ID with quantity = WORKER_COUNT_FILTER_BY_YEAR_ITEMS
for ((i=1; i<=WORKER_COUNT_JOINER_BY_ITEM_ID; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_item_id$i:
    container_name: joiner_by_item_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=JOINER_BY_ITEM_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=max_quantity_profit_items,menu_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_2
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY,$REQUEST_CONTROLLER_COUNT
      - WORKER_MIDDLEWARE_RECEIVERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_ID=$i

EOL
done

# ==============================================================================
# Third Query
# ==============================================================================
WORKER_COUNT_JOINER_BY_STORE_ID=$WORKER_COUNT_FILTER_BY_YEAR

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_SEMESTER; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_semester_worker$i:
    container_name: grouper_by_semester_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=GROUPER_BY_SEMESTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_filtered_by_hour_q2
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_HOUR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=semester_aggregator_queue
      - WORKER_MIDDLEWARE_RECEIVERS=1 # Only one aggregator
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_STORE_ID; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_store_id$i:
    container_name: joiner_by_store_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=JOINER_BY_STORE_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=semester_grouped_transactions,stores
      - WORKER_MIDDLEWARE_SENDERS=1,$REQUEST_CONTROLLER_COUNT
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_3
      - WORKER_MIDDLEWARE_RECEIVERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_ID=$i

EOL
done

# ==============================================================================
# Fourth Query
# ==============================================================================

WORKER_COUNT_GROUPER_BY_STORE_USER=$8
WORKER_COUNT_JOINER_BY_USER_ID=$9
WORKER_COUNT_JOINER_BY_USER_STORE=$9
# WORKER_COUNT_JOINER_BY_STORE_ID=$WORKER_COUNT_FILTER_BY_YEAR

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_STORE_USER; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_store_user_worker$i:
    container_name: grouper_by_store_user_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=GROUPER_BY_STORE_USER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_filtered_by_hour_q2
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=store_user_transactions
      - WORKER_MIDDLEWARE_RECEIVERS=1 # Only one top 3 aggregator
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_STORE_USER; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  aggregator_by_store_user$i:
    container_name: aggregator_by_store_user$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=AGGREGATOR_BY_STORE_USER
      - WORKER_MIDDLEWARE_INPUTQUEUE=store_user_transactions
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_GROUPER_BY_STORE_USER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=top_3_store_users
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_USER_ID
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_ID; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_user_id$i:
    container_name: joiner_by_user_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=JOINER_BY_USER_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=users,top_3_store_users # We first listen to top_3_store_users and then to users
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_CONTROLLER_COUNT,1
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=top_3_users_name
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_USER_STORE
      - WORKER_ID=$i

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_STORE; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_user_store$i:
    container_name: joiner_by_user_store$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=JOINER_BY_USER_STORE
      - WORKER_MIDDLEWARE_INPUTQUEUE=top_3_users_name,stores
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_JOINER_BY_USER_ID,$REQUEST_CONTROLLER_COUNT
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_4
      - WORKER_MIDDLEWARE_RECEIVERS=$REQUEST_CONTROLLER_COUNT
      - WORKER_ID=$i

EOL
done


cat >> "$OUTPUT_FILE" <<EOL
  aggregator_semester_worker1:
    container_name: aggregator_semester_worker1
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
    networks:
      - testing_net
    depends_on:
      - rabbit
    environment:
      - WORKER_JOB=AGGREGATOR_SEMESTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=semester_aggregator_queue
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_GROUPER_BY_SEMESTER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=semester_grouped_transactions
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_STORE_ID
      - WORKER_ID=1

EOL

cat >> "$OUTPUT_FILE" <<EOL
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL

echo "Archivo '$OUTPUT_FILE' creado exitosamente con $WORKER_COUNT_FILTER_BY_YEAR de tipo filter by year y $WORKER_COUNT_FILTER_BY_HOUR de tipo filter by hour y $WORKER_COUNT_FILTER_BY_AMOUNT de tipo filter by amount."
