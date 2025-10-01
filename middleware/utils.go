package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Check if the error is a connection error
func isConnectionError(err error) bool {
	return err == amqp.ErrClosed
}

// Testing Utils

func setupRabbitMQConnection() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("Error connecting to RabbitMQ: %v\n", err)
		panic("Failed to connect to RabbitMQ")
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Error connecting to RabbitMQ: %v\n", err)
		panic("Failed to connect to RabbitMQ")
	}

	return conn, ch
}

func createTestCallback(messages *[]string) onMessageCallback {
	return func(consumeChannel ConsumeChannel, done chan error) {
		if consumeChannel == nil {
			return
		}

		deliveries := (<-chan amqp.Delivery)(*consumeChannel)
		for d := range deliveries {
			*messages = append(*messages, string(d.Body))
			d.Ack(false)
		}
	}
}

func createTestCallbackForMultiple(messages *[]string, expectedCount int) onMessageCallback {
	return func(consumeChannel ConsumeChannel, done chan error) {
		if consumeChannel == nil {
			return
		}

		deliveries := (<-chan amqp.Delivery)(*consumeChannel)
		for d := range deliveries {
			*messages = append(*messages, string(d.Body))
			currentCount := len(*messages)
			d.Ack(false)

			if currentCount >= expectedCount {
				return
			}
		}
	}
}
