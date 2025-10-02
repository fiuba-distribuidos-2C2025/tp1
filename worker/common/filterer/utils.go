package filterer

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkerConfig struct {
	MiddlewareUrl   string
	InputQueue      string
	OutputQueue     string
	InputSenders    int
	OutputReceivers []string
	WorkerJob       string
	ID              int
}

type Worker struct {
	Config   WorkerConfig
	Shutdown chan struct{}
	Conn     *amqp.Connection
	Channel  *amqp.Channel
}
