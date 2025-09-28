#!/usr/bin/env python3

import sys
import time
import pika

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "by_hour_filter_output"
OUT_QUEUE = "by_amount_filter_output"
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 2  # seconds


def main():  # Generate transactions
    transactions_batch_1 = """
    2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,1500.10,2023-07-01 06:00:00
    7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,75.0,2024-07-01 08:25:02
    """

    transactions_batch_2 = """
    4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,20.0,2026-07-01 22:80:21
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,10000.0,2025-07-01 23:00:04
    """

    transactions_batch_3 = """
    9d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,0.0,2024-07-01 22:00:10
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,76.0,2025-07-01 09:10:70
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
        ch.queue_declare(
            queue=IN_QUEUE, durable=True, auto_delete=False, exclusive=False
        )
        ch.queue_declare(
            queue=OUT_QUEUE, durable=True, auto_delete=False, exclusive=False
        )

        ch.basic_publish(
            exchange="",
            routing_key=IN_QUEUE,
            body=str(transactions_batch_1).encode("utf-8"),
        )

        ch.basic_publish(
            exchange="",
            routing_key=IN_QUEUE,
            body=str(transactions_batch_2).encode("utf-8"),
        )

        ch.basic_publish(
            exchange="",
            routing_key=IN_QUEUE,
            body=str(transactions_batch_3).encode("utf-8"),
        )

        # Receive whatever workers forward
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
