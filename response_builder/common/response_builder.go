package common

import (
	"fmt"
	"net"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ResponseBuilderConfig struct {
	MiddlewareUrl string
	WorkerCount   int
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
	log.Info("Starting response builder")
	rabbit_conn, _ := amqp.Dial(m.Config.MiddlewareUrl)
	channel, _ := rabbit_conn.Channel()
	outChan := make(chan string)
	doneChan := make(chan error)
	// for i := 1; i <= m.Config.WorkerCount; i++ {
	// 	log.Infof("Listening on queue results_%d", i)
	// 	doneChan := make(chan error)
	// 	resultsExchange := middleware.NewMessageMiddlewareQueue(fmt.Sprintf("results_%d", i), channel)
	// 	resultsExchange.StartConsuming(createResultsCallback(outChan, doneChan))
	// }
	resultsExchange := middleware.NewMessageMiddlewareQueue("results_1", channel)
	resultsExchange.StartConsuming(createResultsCallback(outChan, doneChan))

	final_result := []string{}
	totalEOFs := 0
	for {
		select {
		case msg := <-outChan:
			if msg == "EOF" {
				log.Info("EOF received")
				totalEOFs++
				log.Infof("TOTAL EOFS: %d, EXPECTED: %d", totalEOFs, m.Config.WorkerCount)
				if totalEOFs == m.Config.WorkerCount {
					finalResultsExchange := middleware.NewMessageMiddlewareQueue("final_results_1", channel)
					finalResultsExchange.Send([]byte(strings.Join(final_result, "\n")))
					log.Info("Total EOFS received, sending query 1 result")
					return nil
				}
				continue
			}

			log.Info("Result received query 1")
			final_result = append(final_result, msg)
		}
	}
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

			case err := <-done:
				log.Errorf("Consumer error: %v", err)
				doneChan <- err
			}
		}
	}
}
