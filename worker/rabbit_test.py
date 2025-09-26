#!/usr/bin/env python3
"""
-rabbit_test.py — quick tester for the Go worker
- Generates N random integers (0-100).
- Sends them to the "input" queue.
- Reads results from the "output" queue and prints them.
"""

import argparse
import random
import sys
import time
import pika

AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "input"
OUT_QUEUE = "output"


def main():
    ap = argparse.ArgumentParser(description="Send N random numbers to worker and read results.")
    ap.add_argument("count", type=int, help="how many random numbers to generate")
    args = ap.parse_args()

    # Generate numbers
    numbers = [random.randint(0, 100) for _ in range(args.count)]
    print("Generated numbers:", numbers)

    try:
        conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(2)

    with conn:
        ch = conn.channel()
        ch.queue_declare(queue=IN_QUEUE, durable=False, auto_delete=False, exclusive=False)
        ch.queue_declare(queue=OUT_QUEUE, durable=False, auto_delete=False, exclusive=False)

        # Send numbers
        for n in numbers:
            ch.basic_publish(exchange="", routing_key=IN_QUEUE, body=str(n).encode("utf-8"))
        print(f"Sent {len(numbers)} numbers to '{IN_QUEUE}'.")

        # Receive whatever workers forward (only evens)
        print("Waiting for forwarded results on output queue...")
        time.sleep(1)  # give workers a moment to process

        results = []
        while True:
            method, props, body = ch.basic_get(queue=OUT_QUEUE, auto_ack=True)
            if not method:
                break
            results.append(int(body.decode("utf-8")))

        print("Results received:", results)


if __name__ == "__main__":
    main()
