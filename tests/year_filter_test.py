#!/usr/bin/env python3

import sys
import time
import pika

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "test-input"
OUT_QUEUE = "test-output"
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 2  # seconds

def main():    # Generate transactions
    transactions_batch = """
    2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00
    7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2024-07-01 07:00:02
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2025-07-01 07:00:04
    4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2026-07-01 07:00:21
    """
    print(transactions_batch)

    # Try to connect to RabbitMQ with retries
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
            break
        except Exception as e:
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                print(f"Failed to connect to RabbitMQ after {MAX_RETRY_ATTEMPTS} attempts: {e}", file=sys.stderr)
                sys.exit(2)
            time.sleep(RETRY_DELAY)

    with conn:
        ch = conn.channel()
        ch.queue_declare(queue=IN_QUEUE, durable=True, auto_delete=False, exclusive=False)
        ch.queue_declare(queue=OUT_QUEUE, durable=True, auto_delete=False, exclusive=False)

        ch.basic_publish(exchange="", routing_key=IN_QUEUE, body=str(transactions_batch).encode("utf-8"))

        # Receive whatever workers forward (only evens)
        print("Waiting for forwarded results on output queue...")
        time.sleep(1)  # give workers a moment to process

        results = ""
        while True:
            method, props, body = ch.basic_get(queue=OUT_QUEUE, auto_ack=True)
            if not method:
                break
            results += body.decode("utf-8")

        print("Results received:\n", results)

if __name__ == "__main__":
    main()
