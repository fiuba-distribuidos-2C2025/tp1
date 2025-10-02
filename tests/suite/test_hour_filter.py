#!/usr/bin/env python3

import sys
import os
import time

# Add the current directory to Python path for importing rabbitmq_utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rabbitmq_utils import connect_with_retry, setup_consumer_queue, send_batches, send_eof

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "transactions_2024_2025"
OUT_QUEUE = "transactions_filtered_by_hour"
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 2  # seconds
NUM_IN_WORKERS = 3
NUM_OUT_WORKERS = 3

def main():  # Generate transactions
    transactions_batch_1 = """
    2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,100,2023-07-01 06:00:00
    7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,100,2024-07-01 08:25:02
    """

    transactions_batch_2 = """
    4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,100,2026-07-01 22:80:21
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,100,2025-07-01 23:00:04
    """

    transactions_batch_3 = """
    9d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,100,2024-07-01 22:00:10
    85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,100,2025-07-01 09:10:70
    """

    # Try to connect to RabbitMQ with retries
    conn = connect_with_retry(AMQP_URL, MAX_RETRY_ATTEMPTS, RETRY_DELAY)

    with conn:
        ch, queue_name = setup_consumer_queue(conn, OUT_QUEUE)

        # Prepare batches
        batches = [transactions_batch_1, transactions_batch_2, transactions_batch_3]

        # Publish each batch to its worker via routing key = worker number ("1"..)
        send_batches(ch, batches, IN_QUEUE, NUM_IN_WORKERS)

        # Send EOF to each worker
        send_eof(ch, IN_QUEUE, NUM_IN_WORKERS)

        # Receive whatever workers forward (only evens)
        print("Waiting for forwarded results on output queue...")
        time.sleep(1)  # give workers a moment to process

        eof_target = NUM_OUT_WORKERS
        eof_count = 0
        def callback(ch, method, properties, body):
            nonlocal eof_count, eof_target
            body_str = body.decode("utf-8").strip()

            if body_str == "EOF":
                eof_count += 1
                print(f"Received EOF ({eof_count}/{eof_target})")
                if eof_count >= eof_target:
                    print("Received all EOFs — stopping consumer.")
                    ch.stop_consuming()  # cleanly exit start_consuming loop
                return
            print(f"{body_str}")

        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        ch.start_consuming()

if __name__ == "__main__":
    main()
