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

def setup_consumer_queue(connection, out_queue):
    ch = connection.channel()
    ch.exchange_declare(exchange=out_queue, exchange_type="topic", durable=True, auto_delete=False)
    result = ch.queue_declare('', exclusive=True)
    queue_name = result.method.queue
    ch.queue_bind(exchange=out_queue, queue=queue_name, routing_key="#")
    return ch, queue_name

def send_batches(channel, batches, exchange, num_workers):
    for i in range(len(batches)):
        channel.basic_publish(
            exchange=exchange,
            routing_key=str(i % num_workers + 1),
            body=str(batches[i]).encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )

def send_eof(channel, exchange, num_workers):
    for i in range(num_workers):
        routing_key = str(i + 1)
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=b"EOF")
