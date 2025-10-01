package common

import (
	"fmt"
	"net"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ResponseBuilderConfig struct {
	MiddlewareUrl string
}

type ResponseBuilder struct {
	Config      ResponseBuilderConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewResponseBuilder(config ResponseBuilderConfig) *ResponseBuilder {
	return &ResponseBuilder{
		Config:      config,
		listener:    nil,
		workerAddrs: nil,
		shutdown:    make(chan struct{}),
	}
}

func (m *ResponseBuilder) Start() error {
	rabbit_conn, _ := amqp.Dial(m.Config.MiddlewareUrl)
	channel, _ := rabbit_conn.Channel()
	resultsExchange1 := middleware.NewMessageMiddlewareQueue("results_1", channel)
	outChan := make(chan string)
	doneChan := make(chan error)
	resultsExchange1.StartConsuming(createResultsCallback(outChan, doneChan))

	select {
	case msg := <-outChan:
		channel, _ = rabbit_conn.Channel()
		log.Info("Resending query 1 result")
		finalResultsExchange := middleware.NewMessageMiddlewareQueue("final_results_1", channel)
		finalResultsExchange.Send([]byte(msg))
	}

	return nil
}

// createResultsCallback creates a callback function for consuming results
func createResultsCallback(resultChan chan string, doneChan chan error) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Info("Results consumer started")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Info("Consume channel closed")
					doneChan <- fmt.Errorf("channel closed unexpectedly")
					return
				}

				log.Infof("Message received - Length: %d, Body: %s", len(msg.Body), string(msg.Body))

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to acknowledge message: %v", err)
					doneChan <- fmt.Errorf("failed to ack message: %w", err)
					return
				}

				resultChan <- string(msg.Body)
				return

			case err := <-done:
				log.Errorf("Consumer error: %v", err)
				doneChan <- err
				return
			}
		}
	}
}
