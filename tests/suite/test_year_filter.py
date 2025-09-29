#!/usr/bin/env python3

import sys
import time
import pika

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "by_year_filter_input"
OUT_QUEUE = "by_year_filter_output"
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 2  # seconds


def main():  # Generate transactions
    if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) <= 0:
        print(f"usage: {sys.argv[0]} <num_workers>", file=sys.stderr)
        sys.exit(2)

    num_workers = int(sys.argv[1])

    transactions_batch = """
    2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00
    7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2024-07-01 07:00:02
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2025-07-01 07:00:04
    4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2026-07-01 07:00:21
    """

    # Try to connect to RabbitMQ with retries
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
            break
        except Exception as e:
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                print(
                    f"Failed to connect to RabbitMQ after {MAX_RETRY_ATTEMPTS} attempts: {e}",
                    file=sys.stderr,
                )
                sys.exit(2)
            time.sleep(RETRY_DELAY)

    with conn:
        ch = conn.channel()

        # Declare the topic exchange where workers are bound with routing keys "1", "2", ..., "N"
        ch.exchange_declare(exchange=OUT_QUEUE, exchange_type="topic", durable=True, auto_delete=False)

        # Results queue to collect whatever workers send back
        ch.queue_declare(queue=IN_QUEUE, durable=True, auto_delete=False, exclusive=False)

        # Prepare batches
        lines = [ln.strip() for ln in transactions_batch.strip().splitlines() if ln.strip()]
        batches = [[] for _ in range(num_workers)]
        for idx, line in enumerate(lines):
            batches[idx % num_workers].append(line)

        # Publish each batch to its worker via routing key = worker number ("1"..)
        for i in range(num_workers):
            routing_key = str(i + 1)
            payload = "\n".join(batches[i]).encode("utf-8")
            if payload:
                ch.basic_publish(
                    exchange=IN_QUEUE,
                    routing_key=routing_key,
                    body=payload,
                    properties=pika.BasicProperties(delivery_mode=2),  # make message persistent
                )

        # Send EOF to each worker
        for i in range(num_workers):
            routing_key = str(i + 1)
            ch.basic_publish(
                exchange=IN_QUEUE,
                routing_key=routing_key,
                body=b"EOF",
            )

        # Receive whatever workers forward (only evens)
        print("Waiting for forwarded results on output queue...")
        time.sleep(1)  # give workers a moment to process

        results = ""
        while True:
            method, props, body = ch.basic_get(queue=OUT_QUEUE, auto_ack=True)
            if not method:
                break
            results += body.decode("utf-8") + "\n"

        print("Results received:\n", results.strip())


if __name__ == "__main__":
    main()
