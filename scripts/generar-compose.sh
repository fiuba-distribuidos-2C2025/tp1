#!/bin/bash

# Validación de argumentos de entrada
if [ $# -lt 2 ]; then
  echo "Uso: $0 <archivo_salida> <archivo_configuracion>"
  exit 1
fi

OUTPUT_FILE="$1"

CONFIG_FILE="$2"
source "$CONFIG_FILE"
REQUEST_HANDLER_SENDER="1"

PROXY_REQUESTHANDLERS_ADDRESSES=""
for ((i=1; i<=REQUEST_CONTROLLER_COUNT; i++)); do
    if [ $i -eq 1 ]; then
        PROXY_REQUESTHANDLERS_ADDRESSES="request_handler$i:8901"
    else
        PROXY_REQUESTHANDLERS_ADDRESSES="$PROXY_REQUESTHANDLERS_ADDRESSES,request_handler$i:8901"
    fi
done

WORKER_ADDRESSES="proxy"

cat > "$OUTPUT_FILE" <<EOL
name: tp1
services:
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
        interval: 1s
        timeout: 1s
        retries: 25
        start_period: 500ms

  proxy:
    container_name: proxy
    image: proxy:latest
    entrypoint: /proxy
    volumes:
      - ./proxy/config.yaml:/config/config.yaml
    environment:
      - PROXY_REQUESTHANDLERS_ADDRESSES=$PROXY_REQUESTHANDLERS_ADDRESSES
    depends_on:
        rabbit:
            condition: service_healthy
        request_handler1:
            condition: service_started
    networks:
      - testing_net

EOL

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Request Handlers
# ==============================================================================

EOL

for ((i=1; i<=REQUEST_CONTROLLER_COUNT; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,request_handler$i"
cat >> "$OUTPUT_FILE" <<EOL
  request_handler$i:
    container_name: request_handler$i
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
      - REQUEST_MIDDLEWARE_RECEIVERS_TRANSACTIONSCOUNT=$WORKER_COUNT_FILTER_BY_YEAR
      - REQUEST_MIDDLEWARE_RECEIVERS_TRANSACTIONITEMSCOUNT=$WORKER_COUNT_FILTER_BY_YEAR_ITEMS
      - REQUEST_MIDDLEWARE_RECEIVERS_STORESQ3COUNT=$WORKER_COUNT_JOINER_BY_STORE_ID
      - REQUEST_MIDDLEWARE_RECEIVERS_STORESQ4COUNT=$WORKER_COUNT_JOINER_BY_USER_STORE
      - REQUEST_MIDDLEWARE_RECEIVERS_MENUITEMSCOUNT=$WORKER_COUNT_JOINER_BY_ITEM_ID
      - REQUEST_MIDDLEWARE_RECEIVERS_USERSCOUNT=$WORKER_COUNT_JOINER_BY_USER_ID
      - REQUEST_IP=request_handler$i
      - REQUEST_PORT=8901
      - REQUEST_ID=$i

EOL
done

for ((i=1; i<=CLIENT_COUNT; i++)); do
cat >> "$OUTPUT_FILE" <<EOL
  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    volumes:
        - ./client:/config
        - ./data:/data
        - ./results:/results
    depends_on:
        rabbit:
            condition: service_healthy
        proxy:
            condition: service_started
    networks:
        - testing_net
    environment:
        - CLIENT_ID=$i

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# First Query
# ==============================================================================

EOL

for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,filter_by_year_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_year_worker$i:
    container_name: filter_by_year_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_filter_by_year_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=YEAR_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_HANDLER_SENDER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_2024_2025_q1,transactions_2024_2025_q4
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_FILTER_BY_HOUR,$WORKER_COUNT_GROUPER_BY_STORE_USER
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_HOUR; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,filter_by_hour_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_hour_worker$i:
    container_name: filter_by_hour_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_filter_by_hour_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=HOUR_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_2024_2025_q1
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_filtered_by_hour_q1,transactions_filtered_by_hour_q3
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_FILTER_BY_AMOUNT,$WORKER_COUNT_GROUPER_BY_SEMESTER
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_AMOUNT; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,filter_by_amount_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_amount_worker$i:
    container_name: filter_by_amount_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_filter_by_amount_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=AMOUNT_FILTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_filtered_by_hour_q1
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_1
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_HOUR
      - WORKER_MIDDLEWARE_RECEIVERS=$RESPONSE_BUILDER_COUNT
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Second Query
# ==============================================================================

EOL

for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR_ITEMS; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,filter_by_year_items_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  filter_by_year_items_worker$i:
    container_name: filter_by_year_items_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_filter_by_year_items_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=YEAR_FILTER_ITEMS
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=transactions_items_2024_2025
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_HANDLER_SENDER
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_GROUPER_BY_YEAR_MONTH
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_YEAR_MONTH; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,grouper_by_year_month_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_year_month_worker$i:
    container_name: grouper_by_year_month_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_grouper_by_year_month_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=GROUPER_BY_YEAR_MONTH
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_items_2024_2025
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=year_month_grouped_items
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR_ITEMS
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,aggregator_by_profit_quantity$i"
cat >> "$OUTPUT_FILE" <<EOL
  aggregator_by_profit_quantity$i:
    container_name: aggregator_by_profit_quantity$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_aggregator_by_profit_quantity_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=AGGREGATOR_BY_PROFIT_QUANTITY
      - WORKER_MIDDLEWARE_INPUTQUEUE=year_month_grouped_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=max_quantity_profit_items
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_GROUPER_BY_YEAR_MONTH
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_ITEM_ID
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

# TODO ADD JOINERS BY ID with quantity = WORKER_COUNT_FILTER_BY_YEAR_ITEMS
for ((i=1; i<=WORKER_COUNT_JOINER_BY_ITEM_ID; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,joiner_by_item_id$i"
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_item_id$i:
    container_name: joiner_by_item_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_joiner_by_item_id_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=JOINER_BY_ITEM_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=max_quantity_profit_items,menu_items
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_2
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY,$REQUEST_HANDLER_SENDER
      - WORKER_MIDDLEWARE_RECEIVERS=$RESPONSE_BUILDER_COUNT
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Third Query
# ==============================================================================

EOL

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_SEMESTER; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,grouper_by_semester_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_semester_worker$i:
    container_name: grouper_by_semester_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_grouper_by_semester_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=GROUPER_BY_SEMESTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_filtered_by_hour_q3
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_HOUR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=semester_aggregator_queue
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_AGGREGATOR_BY_SEMESTER
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_SEMESTER; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,aggregator_semester_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  aggregator_semester_worker$i:
    container_name: aggregator_semester_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_aggregator_semester_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=AGGREGATOR_SEMESTER
      - WORKER_MIDDLEWARE_INPUTQUEUE=semester_aggregator_queue
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_GROUPER_BY_SEMESTER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=semester_grouped_transactions
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_STORE_ID
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_STORE_ID; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,joiner_by_store_id$i"
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_store_id$i:
    container_name: joiner_by_store_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_joiner_by_store_id_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=JOINER_BY_STORE_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=semester_grouped_transactions,stores_q3
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_AGGREGATOR_BY_SEMESTER,$REQUEST_HANDLER_SENDER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_3
      - WORKER_MIDDLEWARE_RECEIVERS=$RESPONSE_BUILDER_COUNT
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Fourth Query
# ==============================================================================

EOL

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_STORE_USER; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,grouper_by_store_user_worker$i"
cat >> "$OUTPUT_FILE" <<EOL
  grouper_by_store_user_worker$i:
    container_name: grouper_by_store_user_worker$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_grouper_by_store_user_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=GROUPER_BY_STORE_USER
      - WORKER_MIDDLEWARE_INPUTQUEUE=transactions_2024_2025_q4
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_FILTER_BY_YEAR
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=store_user_transactions
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_AGGREGATOR_BY_STORE_USER
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_STORE_USER; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,aggregator_by_store_user$i"
cat >> "$OUTPUT_FILE" <<EOL
  aggregator_by_store_user$i:
    container_name: aggregator_by_store_user$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_aggregator_by_store_user_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=AGGREGATOR_BY_STORE_USER
      - WORKER_MIDDLEWARE_INPUTQUEUE=store_user_transactions
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_GROUPER_BY_STORE_USER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=top_3_store_users
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_USER_ID
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_ID; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,joiner_by_user_id$i"
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_user_id$i:
    container_name: joiner_by_user_id$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_joiner_by_user_id_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=JOINER_BY_USER_ID
      - WORKER_MIDDLEWARE_INPUTQUEUE=users,top_3_store_users # We first listen to top_3_store_users and then to users
      - WORKER_MIDDLEWARE_SENDERS=$REQUEST_HANDLER_SENDER,$WORKER_COUNT_AGGREGATOR_BY_STORE_USER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=top_3_users_name
      - WORKER_MIDDLEWARE_RECEIVERS=$WORKER_COUNT_JOINER_BY_USER_STORE
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_STORE; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,joiner_by_user_store$i"
cat >> "$OUTPUT_FILE" <<EOL
  joiner_by_user_store$i:
    container_name: joiner_by_user_store$i
    image: worker:latest
    entrypoint: /worker
    volumes:
      - ./worker/config.yaml:/config.yaml
      - base_dir_joiner_by_user_store_$i:/base_dir
    networks:
      - testing_net
    depends_on:
        rabbit:
            condition: service_healthy
    environment:
      - WORKER_JOB=JOINER_BY_USER_STORE
      - WORKER_MIDDLEWARE_INPUTQUEUE=top_3_users_name,stores_q4
      - WORKER_MIDDLEWARE_SENDERS=$WORKER_COUNT_JOINER_BY_USER_ID,$REQUEST_HANDLER_SENDER
      - WORKER_MIDDLEWARE_OUTPUTQUEUE=results_4
      - WORKER_MIDDLEWARE_RECEIVERS=$RESPONSE_BUILDER_COUNT
      - WORKER_ID=$i
      - WORKER_BASEDIR=/base_dir

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Response Builders
# ==============================================================================

EOL

for ((i=1; i<=RESPONSE_BUILDER_COUNT; i++)); do
WORKER_ADDRESSES="$WORKER_ADDRESSES,response_builder$i"
cat >> "$OUTPUT_FILE" <<EOL
  response_builder$i:
    container_name: response_builder$i
    image: response_builder:latest
    entrypoint: /response_builder
    volumes:
      - ./response_builder/config.yaml:/config/config.yaml
      - base_dir_response_builder_$i:/base_dir
    environment:
      - RESPONSE_MIDDLEWARE_RESULTS1_COUNT=$WORKER_COUNT_FILTER_BY_AMOUNT
      - RESPONSE_MIDDLEWARE_RESULTS2_COUNT=$WORKER_COUNT_JOINER_BY_ITEM_ID
      - RESPONSE_MIDDLEWARE_RESULTS3_COUNT=$WORKER_COUNT_JOINER_BY_STORE_ID
      - RESPONSE_MIDDLEWARE_RESULTS4_COUNT=$WORKER_COUNT_JOINER_BY_USER_STORE
      - RESPONSE_MIDDLEWARE_RECEIVER=$REQUEST_CONTROLLER_COUNT
      - RESPONSE_MIDDLEWARE_ID=$i
    depends_on:
        rabbit:
            condition: service_healthy
    networks:
      - testing_net

EOL
done

cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Watchers
# ==============================================================================

EOL

IFS=',' read -r -a ADDR_ARRAY <<< "$WORKER_ADDRESSES"
TOTAL=${#ADDR_ARRAY[@]}
CHUNK_SIZE=$(( (TOTAL + WATCHER_COUNT - 1) / WATCHER_COUNT ))

# Build all watcher names once
ALL_WATCHERS=()
for ((w=1; w<=WATCHER_COUNT; w++)); do
    ALL_WATCHERS+=( "watcher$w" )
done

for ((i=1; i<=WATCHER_COUNT; i++)); do
    START=$(( (i-1) * CHUNK_SIZE ))
    SLICE=("${ADDR_ARRAY[@]:START:CHUNK_SIZE}")
    SLICE_CSV=$(IFS=','; echo "${SLICE[*]}")

    # Other watchers (all except self)
    OTHER_WATCHERS=("${ALL_WATCHERS[@]:0:i-1}" "${ALL_WATCHERS[@]:i}")
    OTHER_WATCHERS_CSV=$(IFS=','; echo "${OTHER_WATCHERS[*]}")

    # Build combined value, use comma between parts if both are non-empty
    if [[ -n "$SLICE_CSV" && -n "$OTHER_WATCHERS_CSV" ]]; then
        COMBINED="${SLICE_CSV},${OTHER_WATCHERS_CSV}"
    elif [[ -n "$SLICE_CSV" ]]; then
        COMBINED="$SLICE_CSV"
    else
        COMBINED="$OTHER_WATCHERS_CSV"
    fi

cat >> "$OUTPUT_FILE" <<EOL
  watcher$i:
    container_name: watcher$i
    image: watcher:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./watcher/watcher_config.json:/app/watcher_config.json
    depends_on:
        rabbit:
            condition: service_healthy
    restart: unless-stopped
    networks:
      - testing_net
    environment:
      - WORKER_ADDRESSES="$COMBINED"

EOL
done

# Declaración de todos los volúmenes para /base_dir
cat >> "$OUTPUT_FILE" <<EOL
# ==============================================================================
# Volumes
# ==============================================================================

volumes:
EOL

# First Query
for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR; i++)); do
  echo "  base_dir_filter_by_year_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_HOUR; i++)); do
  echo "  base_dir_filter_by_hour_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_FILTER_BY_AMOUNT; i++)); do
  echo "  base_dir_filter_by_amount_$i:" >> "$OUTPUT_FILE"
done

# Second Query
for ((i=1; i<=WORKER_COUNT_FILTER_BY_YEAR_ITEMS; i++)); do
  echo "  base_dir_filter_by_year_items_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_GROUPER_BY_YEAR_MONTH; i++)); do
  echo "  base_dir_grouper_by_year_month_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_PROFIT_QUANTITY; i++)); do
  echo "  base_dir_aggregator_by_profit_quantity_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_ITEM_ID; i++)); do
  echo "  base_dir_joiner_by_item_id_$i:" >> "$OUTPUT_FILE"
done

# Third Query
for ((i=1; i<=WORKER_COUNT_GROUPER_BY_SEMESTER; i++)); do
  echo "  base_dir_grouper_by_semester_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_SEMESTER; i++)); do
  echo "  base_dir_aggregator_semester_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_STORE_ID; i++)); do
  echo "  base_dir_joiner_by_store_id_$i:" >> "$OUTPUT_FILE"
done

# Fourth Query
for ((i=1; i<=WORKER_COUNT_GROUPER_BY_STORE_USER; i++)); do
  echo "  base_dir_grouper_by_store_user_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_AGGREGATOR_BY_STORE_USER; i++)); do
  echo "  base_dir_aggregator_by_store_user_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_ID; i++)); do
  echo "  base_dir_joiner_by_user_id_$i:" >> "$OUTPUT_FILE"
done

for ((i=1; i<=WORKER_COUNT_JOINER_BY_USER_STORE; i++)); do
  echo "  base_dir_joiner_by_user_store_$i:" >> "$OUTPUT_FILE"
done

# Response builders
for ((i=1; i<=RESPONSE_BUILDER_COUNT; i++)); do
  echo "  base_dir_response_builder_$i:" >> "$OUTPUT_FILE"
done

cat >> "$OUTPUT_FILE" <<EOL

# ==============================================================================
# Network
# ==============================================================================

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL

echo "Archivo '$OUTPUT_FILE' creado exitosamente."
