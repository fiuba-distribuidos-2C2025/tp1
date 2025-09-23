#!/usr/bin/env python3
"""
rabbit_test.py — quick tester for your Go worker

- Publishes integers to the "input" queue, then a "fin" message
- Polls the "output" queue once for the accumulated sum
"""

import argparse
import sys
import time
from typing import List, Optional

import pika


AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
IN_QUEUE = "input"
OUT_QUEUE = "output"


def connect(url: str) -> pika.BlockingConnection:
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)


def declare_queues(ch: pika.channel.Channel) -> None:
    # Match the Go code: non-durable, not auto-deleted, not exclusive
    ch.queue_declare(queue=IN_QUEUE, durable=False, auto_delete=False, exclusive=False)
    ch.queue_declare(queue=OUT_QUEUE, durable=False, auto_delete=False, exclusive=False)


def publish_batch(ch: pika.channel.Channel, numbers: List[int]) -> None:
    for n in numbers:
        ch.basic_publish(exchange="", routing_key=IN_QUEUE, body=str(n).encode("utf-8"))
    # End-of-batch marker (your worker looks for literal "fin" in the body)
    ch.basic_publish(exchange="", routing_key=IN_QUEUE, body=b"fin")


def wait_for_sum(
    ch: pika.channel.Channel, timeout_secs: float = 10.0, poll_interval: float = 0.2
) -> Optional[str]:
    """
    Polls OUT_QUEUE until a message arrives or timeout expires.
    Returns the message body as a string, or None on timeout.
    """
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        method, props, body = ch.basic_get(queue=OUT_QUEUE, auto_ack=True)
        if method:
            return body.decode("utf-8", errors="replace")
        time.sleep(poll_interval)
    return None


def main():
    ap = argparse.ArgumentParser(description="Test the Go sum-accumulator worker.")
    ap.add_argument(
        "numbers",
        metavar="N",
        type=int,
        nargs="*",
        help="integers to send in one batch (default: 1 2 3)",
    )
    ap.add_argument(
        "--repeat",
        "-r",
        type=int,
        default=1,
        help="repeat the same batch this many times (default: 1)",
    )
    ap.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=10.0,
        help="seconds to wait for result per batch (default: 10)",
    )
    ap.add_argument(
        "--url",
        default=AMQP_URL,
        help=f"AMQP URL (default: {AMQP_URL})",
    )
    args = ap.parse_args()

    batch = args.numbers if args.numbers else [1, 2, 3]

    try:
        conn = connect(args.url)
    except Exception as e:
        print(f"Failed to connect to RabbitMQ at {args.url}: {e}", file=sys.stderr)
        sys.exit(2)

    with conn:
        ch = conn.channel()
        declare_queues(ch)

        for i in range(1, args.repeat + 1):
            print(f"\n[batch {i}] sending: {batch}")
            publish_batch(ch, batch)

            print(f"[batch {i}] waiting for sum on '{OUT_QUEUE}' (timeout={args.timeout}s)...")
            result = wait_for_sum(ch, timeout_secs=args.timeout)
            if result is None:
                print(f"[batch {i}] ERROR: timed out waiting for output.")
                sys.exit(1)
            print(f"[batch {i}] received sum: {result}")

    print("\nDone.")


if __name__ == "__main__":
    main()
