#!/usr/bin/env python3

import sys
import os
import time

# Add the current directory to Python path for importing rabbitmq_utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rabbitmq_utils import connect_with_retry, setup_consumer_queue, send_batches, send_eof

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "transactions"
OUT_QUEUE = "results_1"
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 2  # seconds
NUM_IN_WORKERS = 1
NUM_OUT_WORKERS = 3

def main():  # Generate transactions

    transactions_batch_1 = """
    8a9b0c1d-2e3f-4567-8901-2345ef67890a,4,2,198,,67.90,8.15,59.75,2024-12-03 10:44:19
    6e7f8a9b-0c1d-4e2f-3456-789012345abc,18,1,,,189.25,0.00,189.25,2024-02-18 19:37:51
    2f3e4d5c-6b7a-4890-1234-567890abcdef,9,3,147,23,112.00,18.50,93.50,2024-08-11 05:22:36
    9c8d7e6f-5a4b-4390-1827-364950a8b7c6,13,2,,,73.60,0.00,73.60,2024-10-25 15:58:14
    5a6b7c8d-9e0f-4123-4567-89abcdef0123,1,1,256,91,245.30,35.80,209.50,2024-04-12 11:15:47
    1e2f3a4b-5c6d-4789-abcd-ef0123456789,16,3,,,134.75,0.00,134.75,2024-07-28 22:03:29
    """

    transactions_batch_2 = """
    b4e8f739-1a2c-4d5e-8f7a-9b3c6d1e4f82,3,2,101,,42.50,5.25,37.25,2026-03-15 14:33:17
    c9f2a847-5b6d-4e8f-9a7c-2d5e8f1b4c73,12,1,,,128.00,0.00,128.00,2024-06-22 09:47:52
    a1b2c3d4-e5f6-4789-abcd-ef1234567890,8,3,205,45,87.75,12.30,75.45,2024-11-08 16:12:38
    f5d8e9c2-3a4b-4567-8901-c2d3e4f5a678,15,2,,,203.40,0.00,203.40,2024-01-29 07:56:24
    e7f8a9b0-c1d2-4e3f-4567-89ab01cd23ef,5,1,312,78,156.80,25.60,131.20,2024-09-14 21:18:45
    d3c4b5a6-9e8f-4071-8529-6f3e4d5c2b1a,21,3,,,94.20,0.00,94.20,2019-05-07 13:29:03
    """

    transactions_batch_3 = """
    3b4c5d6e-7f8a-4901-2345-6789abcdef01,11,2,189,67,91.40,15.20,76.20,2025-01-16 12:41:55
    7f8a9b0c-1d2e-4345-6789-abcd01234567,6,1,,,178.90,0.00,178.90,2024-12-30 06:17:42
    4d5e6f7a-8b9c-4012-3456-789abcdef012,20,3,123,,58.25,4.75,53.50,2021-03-03 18:52:08
    0a1b2c3d-4e5f-4678-9abc-def012345678,14,2,,,167.80,0.00,167.80,2024-09-21 14:26:33
    8e9f0a1b-2c3d-4456-7890-123456789abc,2,1,298,112,215.60,42.30,173.30,2024-06-05 20:39:57
    6c7d8e9f-0a1b-4234-5678-90abcdef1234,19,3,,,83.45,0.00,83.45,2025-11-17 03:14:26
    2a3b4c5d-6e7f-4890-abcd-ef1234567890,10,2,167,89,149.90,22.80,127.10,2024-08-24 17:48:11
    4f5a6b7c-8d9e-4012-3456-789012345def,17,1,,,102.65,0.00,102.65,2024-05-19 23:07:34
    """

    expected_results = [
        "6e7f8a9b-0c1d-4e2f-3456-789012345abc",
        "5a6b7c8d-9e0f-4123-4567-89abcdef0123",
        "1e2f3a4b-5c6d-4789-abcd-ef0123456789",
        "c9f2a847-5b6d-4e8f-9a7c-2d5e8f1b4c73",
        "a1b2c3d4-e5f6-4789-abcd-ef1234567890",
        "f5d8e9c2-3a4b-4567-8901-c2d3e4f5a678",
        "e7f8a9b0-c1d2-4e3f-4567-89ab01cd23ef",
        "3b4c5d6e-7f8a-4901-2345-6789abcdef01",
        "7f8a9b0c-1d2e-4345-6789-abcd01234567",
        "0a1b2c3d-4e5f-4678-9abc-def012345678",
        "8e9f0a1b-2c3d-4456-7890-123456789abc",
        "2a3b4c5d-6e7f-4890-abcd-ef1234567890",
    ]

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

            transactions = [line for line in body_str.split("\n") if line.strip()]
            for tx in transactions:
                tx = tx.strip()
                tx_id = tx.split(",")[0]
                print(f"Transaction {tx} -> valid {tx_id in expected_results}.")

        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        ch.start_consuming()


if __name__ == "__main__":
    main()
