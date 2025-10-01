#!/usr/bin/env python3

import sys
import time
import pika

DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:5672/%2F"
DEFAULT_MAX_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_DELAY = 2  # seconds

def connect_with_retry(
    amqp_url=DEFAULT_AMQP_URL,
    max_retry_attempts=DEFAULT_MAX_RETRY_ATTEMPTS,
    retry_delay=DEFAULT_RETRY_DELAY
):
    for attempt in range(max_retry_attempts):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(amqp_url))
            return conn
        except Exception as e:
            if attempt == max_retry_attempts - 1:
                print(
                    f"Failed to connect to RabbitMQ after {max_retry_attempts} attempts: {e}",
                    file=sys.stderr,
                )
                sys.exit(2)
            time.sleep(retry_delay)

def setup_consumer_queue(connection, queue_name):
    ch = connection.channel()
    ch.queue_declare(queue=queue_name, durable=True, auto_delete=False)
    return ch, queue_name

def send_batches(channel, batches, queue_name, num_workers):
    # Declare worker queues
    worker_queues = []
    for i in range(num_workers):
        worker_queue = f"{queue_name}_{i + 1}"
        channel.queue_declare(queue=worker_queue, durable=True, auto_delete=False)
        worker_queues.append(worker_queue)

    # Distribute batches across worker queues
    for i in range(len(batches)):
        target_queue = worker_queues[i % num_workers]
        channel.basic_publish(
            exchange='',
            routing_key=target_queue,
            body=str(batches[i]).encode("utf-8"),
        )

def send_eof(channel, queue_name, num_workers):
    for i in range(num_workers):
        worker_queue = f"{queue_name}_{i + 1}"
        channel.basic_publish(exchange='', routing_key=worker_queue, body=b"EOF")
